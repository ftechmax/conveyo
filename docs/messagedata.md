# MessageData

`MessageData<T>` lets a message carry a URI for a payload stored outside the Conveyo envelope. Use it for payloads that are too large for normal message bodies: files, binary blobs, CSV exports, compressed feeds, and similar data.

The envelope contains only:

```json
{
  "address": "pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111"
}
```

The payload bytes live in a repository such as Postgres.

## Supported Payload Types

Conveyo hydrates these `MessageData<T>` item types on the consumer side:

| Type | Hydration behavior |
| --- | --- |
| `MessageData<string>` | Reads the payload as UTF-8 text. |
| `MessageData<byte[]>` | Reads the payload into a byte array. |
| `MessageData<Stream>` | Provides a readable stream. The consumer owns disposal. |
| `MessageData<T>` for other reference types | Deserializes JSON using Conveyo's JSON options. |

Hydration requires a settable property. If the message has a read-only `MessageData<T>` property, Conveyo logs a warning and leaves the referenced value unhydrated.

## Register a Repository

Postgres:

```csharp
services.AddConveyo(bus =>
{
    bus.AddPostgresMessageData(
        "Host=localhost;Database=conveyo;Username=app;Password=secret",
        schema: "md",
        chunkSizeBytes: 1_048_576,
        gzip: false);
});
```

The storage package registers:

- A concrete repository.
- `IMessageDataRepository`.
- A hosted service that ensures the storage schema exists at startup.

## Configuration Binding

The storage package can also read from `IConfiguration`.

Postgres keys:

| Key | Required | Default |
| --- | --- | --- |
| `postgres:connection-string` | Yes | n/a |
| `conveyo:storage:schema` | No | `md` |
| `conveyo:storage:chunkSizeBytes` | No | `1048576` |
| `conveyo:storage:gzip` | No | `false` |

```csharp
services.AddConveyo(bus =>
{
    bus.AddPostgresMessageData(configuration);
});
```

## Produce a MessageData Payload

Write the payload stream to `IMessageDataRepository`, then put the returned address in the message.

```csharp
public sealed record UploadRadarImageCommand
{
    public required Guid StationId { get; init; }

    public required string FileName { get; init; }

    public required MessageData<byte[]> Image { get; init; }
}
```

```csharp
public sealed class WeatherSampleUploader(IBus bus, IMessageDataRepository repository)
{
    public async Task UploadRadarImageAsync(
        Guid stationId,
        Stream image,
        string fileName,
        CancellationToken cancellationToken)
    {
        var address = await repository.PutAsync(
            image,
            timeToLive: TimeSpan.FromHours(24),
            cancellationToken);

        await bus.Send(new UploadRadarImageCommand
        {
            StationId = stationId,
            FileName = fileName,
            Image = new MessageData<byte[]>(address)
        }, cancellationToken);
    }
}
```

The repository stores raw bytes. Conveyo does not add JSON framing when you call `PutAsync`.

## Consume a MessageData Payload

On delivery, Conveyo sees the `MessageData<T>` property, reads the referenced payload, and replaces the property value with a hydrated instance.

```csharp
public sealed class RadarImageConsumer : IConsumer<UploadRadarImageCommand>
{
    public Task Consume(ConsumeContext<UploadRadarImageCommand> context)
    {
        var image = context.Message.Image;

        if (!image.HasValue || image.Value is null)
        {
            throw new InvalidOperationException("Radar image was not hydrated.");
        }

        var bytes = image.Value;
        // Process the image bytes.
        return Task.CompletedTask;
    }
}
```

For `MessageData<string>` and `MessageData<byte[]>`, `Value` is already materialized. For `MessageData<Stream>`, dispose the stream when the consumer is done.

## Limits

Consumer hydration is bounded by `MaxMessageDataBytes`. The default is 64 MiB.

```csharp
services.AddConveyo(bus =>
{
    bus.MaxMessageDataBytes(128L * 1024 * 1024);
});
```

If the payload exceeds the limit during hydration, message handling fails. The transport then follows its normal failure path. With RabbitMQ, that means retry and then `<queue>_error`.

RabbitMQ also has an envelope-size limit, configured separately with `host.MaxEnvelopeSizeBytes(...)`. That limit applies to the JSON envelope delivered by the broker, not to bytes loaded from `MessageData`.

## Inline Payloads

Conveyo can read `data:` URIs for small inline payloads:

```csharp
new MessageData<string>(
    new Uri("data:text/plain;base64,U21hbGwgcGF5bG9hZA=="))
```

Inline payloads are decoded directly from the URI. They do not use `IMessageDataRepository`. Keep them small because the whole URI lives inside the message envelope.

## URI Schemes

Storage backends emit canonical URI schemes:

| Backend | Scheme | Example |
| --- | --- | --- |
| Inline payload | `data` | `data:text/plain;base64,U21hbGwgcGF5bG9hZA==` |
| Postgres chunks | `pgbin` | `pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111` |

The full grammar and cross-language resolver rules live in [MessageData URI schemes](messagedata-uris.md).
