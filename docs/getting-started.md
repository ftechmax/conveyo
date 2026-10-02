# Getting Started

You need .NET 10 and a running RabbitMQ broker. This example uses a local broker with the `guest` account on port 5672.

## Create a host

```sh
dotnet new console -n WeatherDemo --framework net10.0
cd WeatherDemo
dotnet add package Microsoft.Extensions.Hosting
dotnet add package Conveyo
dotnet add package Conveyo.RabbitMQ
```

Replace `Program.cs` with:

```csharp
using Conveyo;
using Conveyo.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddConveyo(bus =>
{
    bus.Map<SubmitWeatherObservationCommand>("weather:SubmitWeatherObservationCommand.v1");
    bus.Map<WeatherObservationRecordedEvent>("weather:WeatherObservationRecordedEvent.v1");
    bus.MapEndpointConvention<SubmitWeatherObservationCommand>(new Uri("queue:weather-stations"));
    bus.AddConsumer<SubmitWeatherObservationConsumer>();

    bus.UsingRabbitMq((ctx, rabbit) =>
    {
        rabbit.Host("localhost", "/", host =>
        {
            host.Username("guest");
            host.Password("guest");
        });

        rabbit.ReceiveEndpoint("weather-stations", endpoint =>
            endpoint.ConfigureConsumer<SubmitWeatherObservationConsumer>(ctx));
    });
});

using var host = builder.Build();
await host.StartAsync();

var bus = host.Services.GetRequiredService<IBus>();
await bus.Send(new SubmitWeatherObservationCommand(Guid.NewGuid(), "Vlieland"));
await host.WaitForShutdownAsync();

public sealed record SubmitWeatherObservationCommand(Guid StationId, string Location);
public sealed record WeatherObservationRecordedEvent(Guid StationId, string Location);

public sealed class SubmitWeatherObservationConsumer(
    ILogger<SubmitWeatherObservationConsumer> logger)
    : IConsumer<SubmitWeatherObservationCommand>
{
    public Task Consume(ConsumeContext<SubmitWeatherObservationCommand> context)
    {
        logger.LogInformation("Observation received at {Location}", context.Message.Location);

        return context.Publish(new WeatherObservationRecordedEvent(
            context.Message.StationId,
            context.Message.Location), context.CancellationToken);
    }
}
```

Run `dotnet run`. The consumer logs the observation and publishes an event. Press Ctrl+C to stop.

## Registration and routing

Map every sent, published, or consumed message type with `Map<T>`. Producers and consumers must agree on the URN and payload shape; use a new URN for an incompatible payload change. A consumer's unmapped message type causes registration to fail.

Register consumers with `AddConsumer<T>` and attach them to a receive endpoint with `ConfigureConsumer<T>`. Consumers are scoped services and support constructor injection. Complete bus and host configuration inside the registration callbacks.

| Operation | Configuration | Behavior |
| --- | --- | --- |
| `Send<T>` | `MapEndpointConvention<T>(new Uri("queue:weather-stations"))` | Sends to one queue. If it does not exist, throws `UnroutableMessageException`. |
| `Publish<T>` | `Map<T>("weather:WeatherObservationRecordedEvent.v1")` | Sends to every queue bound to that URN. With no subscribers, the broker drops the event. |

The example has no event subscriber. To receive the event, implement `IConsumer<WeatherObservationRecordedEvent>`, register it, and configure it on a receive endpoint. Different queues each receive a copy; processes consuming the same queue share the work.

A producer-only host needs the mappings and `UsingRabbitMq` host configuration, but no consumer registrations or receive endpoints. Start the host before sending. The receiving process provisions the target queue, so start it before sending commands.

## Consume context

`ConsumeContext<T>` provides the message, cancellation token, message and correlation IDs, receiving queue address, send time, producer host metadata, and application headers. Use `context.Send` or `context.Publish` for follow-up messages to carry inbound correlation and headers. Pass `context.CancellationToken` to asynchronous work so shutdown can cancel it.

Handlers must tolerate duplicate deliveries. Publisher confirmation means broker acceptance, not consumer completion. See [RabbitMQ](rabbitmq.md) for retry and recovery behavior, and [MessageData](messagedata.md) for payloads stored outside the envelope.
