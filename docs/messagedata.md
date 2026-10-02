# MessageData

`MessageData<T>` carries a payload URI instead of embedding a large file or object in the envelope. Its JSON representation is:

```json
{ "address": "pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111" }
```

## Register a repository

Install `Conveyo.Storage.Postgres` and import its namespace. Add storage alongside your bus and transport configuration:

```csharp
bus.AddPostgresMessageData(
    "Host=localhost;Database=conveyo;Username=app;Password=secret",
    schema: "md",
    chunkSizeBytes: 1_048_576,
    gzip: false);
```

Registration creates the schema at host startup and deletes expired payloads every five minutes. Set `cleanupInterval: TimeSpan.Zero` to disable automatic cleanup and schedule `PostgresMessageDataRepository.DeleteExpiredAsync` yourself.

Alternatively, call `bus.AddPostgresMessageData(configuration)` with these keys:

| Key | Required | Default |
| --- | --- | --- |
| `postgres:connection-string` | Yes | — |
| `conveyo:storage:schema` | No | `md` |
| `conveyo:storage:chunkSizeBytes` | No | `1048576` |
| `conveyo:storage:gzip` | No | `false` |
| `conveyo:storage:cleanupInterval` | No | `00:05:00` |

Producer and consumer repositories must use the same database and schema. The URI names the schema and file, not a database host.

## Produce a payload

Inject `IMessageDataRepository` and `IBus`. Map the command and its target queue as in [Getting started](getting-started.md).

```csharp
public sealed record UploadRadarImageCommand
{
    public required MessageData<byte[]> Image { get; init; }
}
```

```csharp
var address = await repository.PutAsync(
    imageStream, timeToLive: TimeSpan.FromHours(24), cancellationToken);

await bus.Send(new UploadRadarImageCommand
{
    Image = new MessageData<byte[]>(address)
}, cancellationToken);
```

`PutAsync` stores the stream's raw bytes and leaves the input stream open. For a JSON object, serialize it into the stream yourself. A new reference has `HasValue == false`; it is hydrated on consumption.

Writes commit metadata and chunks in one transaction, so failed uploads leave no partial file. Chunk sizes are clamped to 64 KiB–4 MiB. With `gzip: true`, writes compress before chunking; reads decode according to the stored encoding, regardless of the reader's write options.

Expired payloads are unavailable as soon as their TTL elapses, before cleanup. Choose a TTL that covers queue delays and retries.

## Consume a payload

Conveyo hydrates settable `MessageData<T>` properties before invoking consumers. Read-only properties are left unhydrated with a warning. The repository is resolved from the delivery scope; scoped and singleton registrations both work.

| Property type | Hydrated value |
| --- | --- |
| `MessageData<string>` | UTF-8 text. |
| `MessageData<byte[]>` | Byte array. |
| `MessageData<Stream>` | Readable stream owned by the consumer. |
| `MessageData<T>` for other reference types | JSON deserialized with Conveyo's JSON options. |

Check `HasValue` or `Value` before using a payload:

```csharp
var bytes = context.Message.Image.Value
    ?? throw new InvalidOperationException("Radar image was not hydrated.");
```

For `MessageData<Stream>`, consume and dispose the stream before returning:

```csharp
await using var stream = context.Message.Feed.Value
    ?? throw new InvalidOperationException("Feed was not hydrated.");
await stream.CopyToAsync(outputStream, context.CancellationToken);
```

A Postgres read stream holds a database connection until disposal, including after failed or partial reads. This also applies to direct `repository.GetAsync` calls. Conveyo disposes source streams when materializing text, bytes, or JSON and cleans up opened streams if hydration fails. Multiple consumers on one delivery share the hydrated message and streams.

## Limits

Consumer hydration defaults to 64 MiB of decoded bytes, including gzip payloads. Configure it inside `AddConveyo`:

```csharp
bus.MaxMessageDataBytes(128L * 1024 * 1024);
```

Materialized payloads fail during hydration when oversized. Stream payloads fail when reading past the limit. Handling then follows the transport's failure path; with RabbitMQ, retry and eventually `<queue>_error`. Direct repository calls have no size limit.

The RabbitMQ envelope limit (`host.MaxEnvelopeSizeBytes(...)`) applies separately to the JSON body, not the referenced bytes.

## Inline payloads

Small payloads can use base64 `data:` URIs without a repository:

```csharp
new MessageData<string>(new Uri("data:text/plain;base64,U21hbGwgcGF5bG9hZA=="))
```

The whole URI counts toward the envelope size limit. See [MessageData URI schemes](messagedata-uris.md) for locator validation and the Postgres [storage layout](messagedata-uris.md#storage-layout).
