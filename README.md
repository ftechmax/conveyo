# Conveyo

> [!NOTE]
> Conveyo is pre-1.0. It is published as preview packages (`0.1.0-preview.N`) so the API can harden through real use. The public API and the JSON/RabbitMQ wire contract may change before 1.0; wire-contract changes are called out in the release notes. I run it in my own services first. Feedback and issues are welcome.

Conveyo helps you build event-driven .NET applications without tying your application code to a broker client. Define messages, write consumers, and use one bus abstraction to send commands, publish events, and pass large payloads by reference.

The bits you interact with is minimalistic by design:

- `Send<T>` routes a command to one queue.
- `Publish<T>` emits an event to zero, one, or many queues.
- `IConsumer<T>` handles messages inside normal .NET dependency-injection scopes.

For transport and blob storage there are additional packages:

| Package | Use it for |
| --- | --- |
| `Conveyo` | `IBus`, `IConsumer<T>`, `ConsumeContext<T>`, envelopes, and hosting. |
| `Conveyo.RabbitMQ` | RabbitMQ transport, publisher confirms, retries, `_error` and `_skipped` queues. |
| `Conveyo.Storage.Postgres` | `MessageData<T>` storage in Postgres `bytea` chunks. |

## Quick Start

Install the core package and one transport:

```bash
dotnet add package Conveyo
dotnet add package Conveyo.RabbitMQ
```

Define messages as plain reference types:

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

Register the bus in your host:

```csharp
services.AddConveyo(bus =>
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
        {
            endpoint.ConfigureConsumer<SubmitWeatherObservationConsumer>(ctx);
        });
    });
});
```

Handle messages with `IConsumer<T>`:

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

Send or publish from application services by injecting `IBus`:

```csharp
public sealed class WeatherStationClient(IBus bus)
{
    public Task SubmitObservationAsync(
        Guid stationId,
        string location,
        CancellationToken cancellationToken)
    {
        return bus.Send(new SubmitWeatherObservationCommand
        {
            StationId = stationId,
            Location = location,
            HumidityPercent = 91,
            WindSpeedMs = 8.4f,
            PressureHpa = 1007.2,
            IsPrecipitating = true
        }, cancellationToken);
    }
}
```

## Documentation

- [Getting started](docs/getting-started.md): registration, consumers, command routing, and events.
- [RabbitMQ transport](docs/rabbitmq.md): host options, TLS, topology, retries, and failure queues.
- [MessageData](docs/messagedata.md): storing large payloads outside the envelope.
- [Wire contract](docs/wire-contract.md): JSON envelope and RabbitMQ wire shape for non-.NET clients.
- [MessageData URI schemes](docs/messagedata-uris.md): `data:` and `pgbin://` resolver contracts.

The sample producer and consumer in [`examples/`](examples/) show RabbitMQ with optional Postgres `MessageData` storage.
