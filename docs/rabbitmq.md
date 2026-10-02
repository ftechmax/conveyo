# RabbitMQ Transport

Register `Conveyo.RabbitMQ` as shown in [Getting started](getting-started.md). Configure the host inside `rabbit.Host(...)`; omitting it fails registration.

## Host options

```csharp
rabbit.Host("rabbitmq.internal", "/", host =>
{
    host.Username("app");
    host.Password("secret");
    host.MaxRetries(5);
    host.PrefetchCount(32);
    host.MaxEnvelopeSizeBytes(2 * 1024 * 1024);
});
```

| Option | Default | Meaning |
| --- | --- | --- |
| `Username` / `Password` | Empty | Broker credentials. |
| `Port` | `5672` | AMQP port. |
| `MaxRetries` | `3` | Retries after the initial consumer attempt. |
| `PrefetchCount` | `16` | Per-channel prefetch; `0` means unlimited. |
| `MaxEnvelopeSizeBytes` | `1 MiB` | Maximum inbound envelope size. |
| `IncludeFaultExceptionDetails` | `false` | Include exception messages and stack traces in terminal headers. |

These options are also settable properties on `RabbitMqHostOptions` (`MaxRetries(...)` sets `MaxRetryCount`). Other properties are:

| Property | Default | Meaning |
| --- | --- | --- |
| `ConsumerDispatchConcurrency` | `1` | Concurrent deliveries on the shared consumer channel. At `1`, retry delays block other queues on that channel; raising it sacrifices ordering. |
| `NetworkRecoveryInterval` | `5s` | Delay between reconnection attempts after an established connection drops. |
| `RequestedHeartbeat` | `60s` | AMQP heartbeat; zero disables it. |
| `InitialConnectionTimeout` | `2m` | Initial connection retry window; zero fails on the first attempt. |
| `InitialConnectionRetryDelay` | `2s` | Initial retry delay, doubled on each failure. |
| `InitialConnectionMaxRetryDelay` | `30s` | Maximum initial connection retry delay. |

## TLS

Call `host.UseSsl()` for AMQPS. It changes port 5672 to 5671 and preserves other port values. Configure certificates through the callback:

```csharp
host.UseSsl(ssl =>
{
    ssl.ServerName = "rabbitmq.internal";
    ssl.CertificatePath = "client.pfx";
    ssl.CertificatePassphrase = certificatePassword;
});
```

| TLS option | Purpose |
| --- | --- |
| `ServerName` | SNI and certificate name matching; defaults to the broker host name. |
| `Protocol` | `SslProtocols` value; by default the OS chooses. |
| `CertificatePath` / `CertificatePassphrase` | Client certificate on disk. |
| `Certificate` | Loaded `X509Certificate`. |
| `UseCertificateAsAuthenticationIdentity` | EXTERNAL authentication with the client certificate. |
| `AllowPolicyErrors(...)` / `EnforcePolicyErrors(...)` | Allow or disallow specific `SslPolicyErrors`. |
| `CertificateSelectionCallback` | Select a client certificate. |
| `CertificateValidationCallback` | Custom server validation; overrides allowed policy errors. |
| `TrustServerCertificate()` | Bypass server certificate validation for local development. |

## Topology

At startup, a consumer declares its receive queue, queue exchange, consumed URN exchanges, and bindings. A producer declares mapped URN exchanges but does not provision target queues. Exchanges are durable fanout exchanges; queues are durable, non-exclusive, and not auto-delete.

`Send<T>` uses mandatory routing to a target queue and throws `UnroutableMessageException` if it is missing. `Publish<T>` uses the URN exchange and succeeds even without subscribers. Both wait for publisher confirmation; neither waits for consumer completion.

## Retries and failure queues

Dispatch failures are retried with delays of 1s, 2s, 4s, and so on. Each attempt deserializes a fresh message and runs all handlers in a new scope. Acknowledgement failures are not retried through the dispatcher. Handlers must tolerate duplicates, including follow-up messages sent by an earlier attempt.

| Delivery outcome | Destination |
| --- | --- |
| Retry exhaustion | `<queue>_error`, original body preserved. |
| Invalid envelope | `<queue>_error`, original body preserved; no retry. |
| Oversized envelope | `<queue>_error`, empty body; no retry. |
| No matching consumer or `MessageNotConsumedException` | `<queue>_skipped`; no retry. |

Conveyo declares the terminal queue, publishes through the default exchange with mandatory routing, and acknowledges the original only after confirmation without a return. It uses its own diagnostic headers rather than broker dead-lettering. See [failure paths](wire-contract.md#25-failure-paths) for header names and values.

If terminal publication fails, the original remains unacknowledged until channel closure or connection recovery permits redelivery. Failure between terminal confirmation and acknowledgement can produce duplicate terminal copies. Shutdown cancels dispatch and retry delays, leaving active deliveries unacknowledged.

## Fault messages

After retry exhaustion, Conveyo also attempts to publish `Fault<T>` to `<original-urn>.fault`. Fault publication is best effort; the `_error` copy is the authoritative failure record. Configure an `IConsumer<Fault<T>>` on a receive endpoint to observe faults for a mapped message type.

Exception details are redacted by default. For local debugging, enable `bus.IncludeFaultExceptionDetails()` inside `AddConveyo` for fault payloads and `host.IncludeFaultExceptionDetails()` inside `rabbit.Host` for terminal headers. These expose exception messages and stack traces; fault payloads also include inner exceptions.

## Lifecycle and recovery

Initial connection attempts are retried within `InitialConnectionTimeout`. Startup fails with the last exception when there is insufficient time for another retry. The RabbitMQ client recovers established connections, topology, QoS, and consumers after an outage. Outgoing publishes during an outage fail and are not replayed automatically; a lost connection can leave a publish outcome uncertain.

Startup and shutdown dispose acquired channels and connections. Fault exchanges and terminal queues are redeclared on the next publication.

See the [wire contract](wire-contract.md) for JSON, AMQP properties, tracing, and interoperability rules.
