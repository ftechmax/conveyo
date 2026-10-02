# Conveyo

Conveyo is a .NET 10 messaging library for sending commands, publishing events, and passing large payloads by reference.

> [!NOTE]
> Conveyo is pre-1.0. The public API and wire contract may change before 1.0. I run it in my own services first. Feedback and issues are welcome.

- `Send<T>` routes a command to one queue.
- `Publish<T>` emits an event to zero, one, or many queues.
- `IConsumer<T>` handles messages inside dependency-injection scopes.

| Package | Purpose |
| --- | --- |
| `Conveyo` | Bus, consumers, and hosting. |
| `Conveyo.RabbitMQ` | RabbitMQ transport, retries, and failure queues. |
| `Conveyo.Storage.Postgres` | Optional storage for large `MessageData<T>` payloads. |

## Getting started

```sh
dotnet add package Conveyo
dotnet add package Conveyo.RabbitMQ
```

Follow [Getting started](docs/getting-started.md) for a runnable command consumer and event publishing. The [weather applications](examples/) show separate producer and consumer processes with Postgres payload storage.

## Documentation

- [RabbitMQ](docs/rabbitmq.md): configuration, TLS, retries, and recovery.
- [MessageData](docs/messagedata.md): storing and consuming large payloads.
- [Wire contract](docs/wire-contract.md): envelope and routing rules for other clients.
- [MessageData URI schemes](docs/messagedata-uris.md): locator validation and Postgres storage layout.
- [Development and testing](docs/testing.md): test commands, smoke tests, and local packages.
