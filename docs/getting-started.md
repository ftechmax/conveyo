# Getting Started

A Conveyo application is built from messages, consumers, and a transport. Register it with `AddConveyo`, map every message type to a stable URN, add consumers, then choose where those messages move.

## Install

```bash
dotnet add package Conveyo
dotnet add package Conveyo.RabbitMQ
```

Add a storage package only when your messages contain `MessageData<T>` payloads:

```bash
dotnet add package Conveyo.Storage.Postgres
```

## Messages

Messages are plain reference types. Records work well because Conveyo serializes the payload as JSON inside its envelope.

```csharp
public sealed record SubmitWeatherObservationCommand
{
    public Guid ObservationId { get; init; } = Guid.NewGuid();
    public required Guid StationId { get; init; }
    public required string Location { get; init; }
    public required int HumidityPercent { get; init; }
    public required float WindSpeedMs { get; init; }
    public required double PressureHpa { get; init; }
    public required bool IsPrecipitating { get; init; }
    public DateTime ObservedAt { get; init; } = DateTime.UtcNow;
}

public sealed record WeatherObservationRecordedEvent
{
    public required Guid ObservationId { get; init; }
    public required Guid StationId { get; init; }
    public required string Location { get; init; }
    public required double FeelsLikeC { get; init; }
    public required string Summary { get; init; }
    public DateTime RecordedAt { get; init; } = DateTime.UtcNow;
}
```

Map each message to a URN. The URN is part of the wire contract, so treat it like a public API.

```csharp
services.AddConveyo(bus =>
{
    bus.Map<SubmitWeatherObservationCommand>("weather:SubmitWeatherObservationCommand.v1");
    bus.Map<WeatherObservationRecordedEvent>("weather:WeatherObservationRecordedEvent.v1");
});
```

A consumed message type must be mapped. `AddConveyo` throws during startup if a consumer handles a message with no URN.

## Consumers

Consumers implement `IConsumer<T>`. They are registered as scoped services, so constructor injection works as expected.

```csharp
public sealed class SubmitWeatherObservationConsumer(IWeatherService weather)
    : IConsumer<SubmitWeatherObservationCommand>
{
    public async Task Consume(ConsumeContext<SubmitWeatherObservationCommand> context)
    {
        var observation = await weather.RecordAsync(
            context.Message,
            context.CancellationToken);

        await context.Publish(new WeatherObservationRecordedEvent
        {
            ObservationId = observation.ObservationId,
            StationId = observation.StationId,
            Location = observation.Location,
            FeelsLikeC = observation.FeelsLikeC,
            Summary = observation.Summary
        }, context.CancellationToken);
    }
}
```

`ConsumeContext<T>` exposes:

| Property | Meaning |
| --- | --- |
| `Message` | The deserialized payload. |
| `MessageId` | Publisher-assigned message id, when present. |
| `CorrelationId` | Correlation id propagated from the inbound message. |
| `DestinationAddress` | The queue address that received the delivery. |
| `SentTime` | Publisher UTC timestamp. |
| `Host` | Publisher host metadata. |
| `Headers` | Application headers propagated to outgoing sends and publishes. |
| `CancellationToken` | Token for the current delivery. |

Use `context.Publish` or `context.Send` inside consumers when follow-up messages should inherit inbound correlation and headers.

## Commands

Commands are routed to a queue with `MapEndpointConvention<T>`.

```csharp
bus.Map<SubmitWeatherObservationCommand>("weather:SubmitWeatherObservationCommand.v1");
bus.MapEndpointConvention<SubmitWeatherObservationCommand>(new Uri("queue:weather-stations"));
```

`IBus.Send<T>` publishes to the RabbitMQ default exchange with `mandatory=true`. If the target queue does not exist, Conveyo throws `UnroutableMessageException`. This is intentional: commands should fail loudly when no handler queue has been provisioned.

## Events

Events are published by URN.

```csharp
await bus.Publish(new WeatherObservationRecordedEvent
{
    ObservationId = observationId,
    StationId = stationId,
    Location = "Vlieland",
    FeelsLikeC = 6.8,
    Summary = "Cold rain, strong western wind"
}, cancellationToken);
```

With RabbitMQ, Conveyo publishes events to a durable fanout exchange named after the message URN. Every receiving queue that configures a consumer for that message is bound to the URN exchange.

If no queue is bound, `Publish<T>` completes successfully and the broker drops the event. Use `Send<T>` for work that must have a known target queue.

## RabbitMQ Setup

A consumer process usually maps messages, adds consumers, and declares a receive endpoint:

```csharp
services.AddConveyo(bus =>
{
    bus.Map<SubmitWeatherObservationCommand>("weather:SubmitWeatherObservationCommand.v1");
    bus.Map<WeatherObservationRecordedEvent>("weather:WeatherObservationRecordedEvent.v1");

    bus.AddConsumer<SubmitWeatherObservationConsumer>();

    bus.UsingRabbitMq((ctx, rabbit) =>
    {
        rabbit.Host("localhost", "/", host =>
        {
            host.Username("guest");
            host.Password("guest");
        });

        rabbit.ReceiveEndpoint("weather-stations", endpoint =>
        {
            endpoint.ConfigureConsumer<SubmitWeatherObservationConsumer>(ctx);
        });
    });
});
```

A producer-only process still maps the message types it sends or publishes. For commands, it also maps the target queue:

```csharp
services.AddConveyo(bus =>
{
    bus.Map<SubmitWeatherObservationCommand>("weather:SubmitWeatherObservationCommand.v1");
    bus.MapEndpointConvention<SubmitWeatherObservationCommand>(new Uri("queue:weather-stations"));

    bus.UsingRabbitMq((_, rabbit) =>
    {
        rabbit.Host("localhost", "/", host =>
        {
            host.Username("guest");
            host.Password("guest");
        });
    });
});
```

See [RabbitMQ transport](rabbitmq.md) for TLS, retry, topology, and failure queue details.

## Large Payloads

Use `MessageData<T>` when a message should carry a reference to a large payload instead of putting the bytes in the envelope.

```csharp
services.AddConveyo(bus =>
{
    bus.AddPostgresMessageData(
        "Host=localhost;Database=conveyo;Username=app;Password=secret");

    bus.Map<UploadSatelliteFeedCommand>("weather:UploadSatelliteFeedCommand.v1");
    bus.MapEndpointConvention<UploadSatelliteFeedCommand>(new Uri("queue:weather-stations"));
});
```

```csharp
public sealed record UploadSatelliteFeedCommand
{
    public required Guid StationId { get; init; }
    public required string FeedName { get; init; }
    public required MessageData<Stream> Feed { get; init; }
}
```

Producers write the stream through `IMessageDataRepository` and put the returned address in the message:

```csharp
var address = await repository.PutAsync(feedStream, TimeSpan.FromHours(24), cancellationToken);

await bus.Send(new UploadSatelliteFeedCommand
{
    StationId = stationId,
    FeedName = "noaa-19-pass-8421",
    Feed = new MessageData<Stream>(address)
}, cancellationToken);
```

Consumers receive a hydrated `MessageData<T>` value when a matching repository is registered:

```csharp
public sealed class SatelliteFeedConsumer : IConsumer<UploadSatelliteFeedCommand>
{
    public async Task Consume(ConsumeContext<UploadSatelliteFeedCommand> context)
    {
        await using var feed = context.Message.Feed.Value;
        // Read the stream here.
    }
}
```

See [MessageData](messagedata.md) for supported payload types, storage backends, and limits.
