# RabbitMQ Transport

`Conveyo.RabbitMQ` is the RabbitMQ transport for Conveyo. It owns connection management, topology declaration, send and publish endpoints, retry handling, and the `_error` and `_skipped` queues.

## Registration

```csharp
services.AddConveyo(bus =>
{
    bus.Map<SubmitWeatherObservationCommand>("weather:SubmitWeatherObservationCommand.v1");
    bus.AddConsumer<SubmitWeatherObservationConsumer>();

    bus.UsingRabbitMq((ctx, rabbit) =>
    {
        rabbit.Host("rabbitmq.internal", "/", host =>
        {
            host.Username("app");
            host.Password("secret");
        });

        rabbit.ReceiveEndpoint("weather-stations", endpoint =>
        {
            endpoint.ConfigureConsumer<SubmitWeatherObservationConsumer>(ctx);
        });
    });
});
```

`UsingRabbitMq` requires a host. If `rabbit.Host(...)` is not called, startup fails.

## Host Options

Host configuration is done inside `rabbit.Host`.

```csharp
rabbit.Host("rabbitmq.internal", "/", host =>
{
    host.Username("app");
    host.Password("secret");
    host.Port(5672);
    host.MaxRetries(5);
    host.PrefetchCount(32);
    host.MaxEnvelopeSizeBytes(2 * 1024 * 1024);
});
```

| Option | Default | Meaning |
| --- | --- | --- |
| `Username` | `""` | RabbitMQ username. |
| `Password` | `""` | RabbitMQ password. |
| `Port` | `5672` | AMQP port. |
| `MaxRetries` | `3` | Consumer retry count before the message is moved to `_error`. |
| `PrefetchCount` | `16` | Per-channel consumer prefetch. `0` means unlimited. |
| `MaxEnvelopeSizeBytes` | `1 MiB` | Maximum inbound envelope body size. Oversized bodies are moved to `_error` without copying the original body. |
| `IncludeFaultExceptionDetails` | `false` | Include exception messages, stack traces, and inner exceptions in broker-visible fault metadata. Use only when that data is safe to expose. |

`RabbitMqHostOptions` also exposes `NetworkRecoveryInterval` and `RequestedHeartbeat` as settable properties.

## TLS

The bus connects over plain AMQP by default. Call `UseSsl()` to use AMQPS. If the port is still `5672`, Conveyo changes it to `5671`; an explicit `Port(...)` value is preserved.

```csharp
rabbit.Host("rabbitmq.internal", "/", host =>
{
    host.Username("app");
    host.Password("secret");
    host.UseSsl();
});
```

The SSL configurator exposes the underlying .NET TLS knobs:

| Knob | Purpose |
| --- | --- |
| `ServerName` | Override SNI and certificate name matching. Defaults to the host name. |
| `Protocol` | Pin an `SslProtocols` value. The default lets the OS choose. |
| `CertificatePath` / `CertificatePassphrase` | Load a client certificate from disk. |
| `Certificate` | Provide a loaded `X509Certificate`. |
| `UseCertificateAsAuthenticationIdentity` | Enable EXTERNAL authentication with the client certificate. |
| `AllowPolicyErrors(...)` / `EnforcePolicyErrors(...)` | Toggle specific `SslPolicyErrors` flags. |
| `CertificateSelectionCallback` | Select a client certificate manually. |
| `CertificateValidationCallback` | Validate the server certificate manually. This overrides `AllowPolicyErrors`. |
| `TrustServerCertificate()` | Accept any server certificate. Use only for local development or trusted internal networks. |

For a self-signed broker certificate:

```csharp
host.UseSsl(ssl => ssl.TrustServerCertificate());
```

For narrower validation, allow only the expected errors:

```csharp
host.UseSsl(ssl => ssl.AllowPolicyErrors(
    SslPolicyErrors.RemoteCertificateChainErrors
    | SslPolicyErrors.RemoteCertificateNameMismatch));
```

## Topology

Conveyo declares the main RabbitMQ topology during hosted service startup. Terminal failure queues are declared lazily the first time a message needs them.

| Startup path | Declares |
| --- | --- |
| Consumer process | The receive queue, the queue exchange, every consumed URN exchange, and the bindings between them. |
| Producer process | Every mapped URN exchange. It does not declare consumer queues. |

All Conveyo exchanges are `fanout`, durable, and not auto-delete. All queues are durable, non-exclusive, and not auto-delete.

For each receive endpoint named `<queue>`:

| Object | Name |
| --- | --- |
| Main exchange | `<queue>` |
| Main queue | `<queue>` |
| Message exchange | The message URN, such as `weather:WeatherObservationRecordedEvent.v1` |
| Error queue | `<queue>_error`, declared lazily on first failed message |
| Skipped queue | `<queue>_skipped`, declared lazily on first skipped message |

Conveyo does not use RabbitMQ's `x-dead-letter-exchange` argument. Failed and skipped messages are published by the consumer process directly to the sibling queue so Conveyo can attach its own discriminator headers.

## Send vs Publish

| Operation | RabbitMQ path | Mandatory | Missing route behavior |
| --- | --- | --- | --- |
| `Publish<T>` | Publishes to the message URN fanout exchange. | `false` | The broker drops the event if no queue is bound. |
| `Send<T>` | Publishes to the default exchange with the target queue as routing key. | `true` | The broker returns the message and Conveyo throws `UnroutableMessageException`. |

Use `Send<T>` for commands that require a known queue. Use `Publish<T>` for events where zero, one, or many consumers may be valid.

## Retries and Failure Queues

When a consumer throws, Conveyo retries the delivery up to `MaxRetries` times with exponential delays: 1 second, 2 seconds, 4 seconds, and so on.

After the final failure, Conveyo declares `<queue>_error` if needed, publishes the original message body to that queue, then acknowledges the original delivery. The `_error` copy includes headers such as:

| Header | Meaning |
| --- | --- |
| `conveyo-outcome` | `faulted` |
| `conveyo-fault-original-queue` | Queue that received the original delivery. |
| `conveyo-fault-reason` | `exception`, `deserialization-failed`, or `envelope-too-large`. |
| `conveyo-fault-exception-type` | Top-level exception type. |
| `conveyo-fault-exception-message` | Redacted by default. |
| `conveyo-fault-attempts` | Number of attempts. |
| `conveyo-fault-timestamp` | UTC fault timestamp. |

Messages that deserialize but have no matching registered consumer are published to lazily declared `<queue>_skipped` with `conveyo-skipped-*` headers.

## Fault Messages

Conveyo also supports `Fault<T>` side-channel events. When a consumed message fails and the original message type is mapped, Conveyo can publish a `Fault<T>` envelope with fault metadata. Fault exception details are redacted by default.

For local debugging:

```csharp
services.AddConveyo(bus =>
{
    bus.IncludeFaultExceptionDetails();

    bus.UsingRabbitMq((_, rabbit) =>
    {
        rabbit.Host("localhost", "/", host =>
        {
            host.Username("guest");
            host.Password("guest");
            host.IncludeFaultExceptionDetails();
        });
    });
});
```

Do not enable exception detail output on brokers where stack traces or exception messages may expose secrets.

## Wire Contract

The exact JSON envelope, AMQP properties, topology rules, and cross-language requirements live in [the wire contract](wire-contract.md).
