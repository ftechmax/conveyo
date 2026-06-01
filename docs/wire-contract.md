# Conveyo wire contract

This document is the cross-language specification for the Conveyo
RabbitMQ wire format. A Go, Rust, Python, or Node.js client that follows
the rules below can produce and consume messages that interoperate with
the .NET implementation in this repository.

If anything here disagrees with the JSON fixtures under
[`tests/Conveyo.RabbitMQ.Test/GoldenEnvelopes/`](../tests/Conveyo.RabbitMQ.Test/GoldenEnvelopes/),
**the fixtures are authoritative** — they are pinned by `EnvelopeGoldenTests`
and any change to them is an intentional wire-contract change.

## 1. Envelope

### 1.1 Encoding

- Every message body is a single JSON object — the **envelope**.
- Encoding is UTF-8. No BOM. Compact (no indentation, no trailing newline).
- Property names are camelCase. Property reads are case-insensitive on the
  .NET side, but new senders MUST emit camelCase.
- Null-valued fields are emitted, not omitted (e.g. `"headers": null`).
- The AMQP `content-type` is `application/json`. Messages are published with
  `delivery-mode = 2` (persistent).

### 1.2 Fields

| Field                | JSON type           | Required | Semantics |
| -------------------- | ------------------- | -------- | --------- |
| `envelopeVersion`    | string              | Yes      | Currently `"1"`. See [§4 Versioning](#4-versioning). |
| `messageId`          | string (UUID) \| null | No     | Application-assigned message id, distinct from the AMQP transport `message-id`. |
| `correlationId`      | string (UUID) \| null | No     | Default propagation key carried with the message across produce/consume. |
| `destinationAddress` | string (URI) \| null  | No     | Set by the consumer to `queue:<queueName>` on receive. Producers MAY leave this null. |
| `messageType`        | array of string     | Yes      | One or more URNs identifying the message type. Most-specific first; see [§1.4](#14-messagetype-urns). |
| `message`            | object              | Yes      | The user payload. Shape is defined per `messageType`. |
| `sentTime`           | string (RFC 3339) \| null | No | When the producer serialized the envelope, UTC. |
| `headers`            | object (string→string) \| null | No | Application-defined string headers. Distinct from AMQP headers. |
| `host`               | object \| null      | No       | Producer host info; see [§1.5](#15-host). |

### 1.3 Timestamps

`sentTime` is an RFC 3339 / ISO 8601 string, always in UTC with a `Z`
suffix. The .NET serializer omits trailing zero fractional seconds
(e.g. `2026-05-14T12:34:56.789Z`, not `2026-05-14T12:34:56.7890000Z`).
Cross-language senders MAY include any sub-second precision; the .NET
consumer parses with
`DateTimeOffset.Parse(..., styles: AssumeUniversal | AdjustToUniversal)`.

### 1.4 `messageType` URNs

- The array MUST contain at least one entry.
- Entry 0 is the **primary URN** — the one this envelope is published with
  on the wire and the name of the corresponding RabbitMQ exchange.
- Additional entries declare the message's supertypes for routing on
  consumers that subscribe to a less-specific URN. Order goes from most-
  specific to least-specific.
- URNs often use a domain-oriented scheme (e.g.
  `weather:WeatherObservationRecordedEvent.v2`), but Conveyo does not validate
  the scheme — any non-empty string is accepted as a routing key.

### 1.5 `host`

```jsonc
{
  "machineName": "golden-host",
  "processName": "Conveyo.GoldenTests",
  "processId": 4242,
  "assembly": "Conveyo.Test",
  "conveyoVersion": "0.0.0-golden",
  "operatingSystemVersion": "Unix 6.1.0",
  "runtime": "dotnet",
  "runtimeVersion": "10.0.0"
}
```

`runtime` and `runtimeVersion` are the cross-language identity. A Go
sender SHOULD emit `"runtime": "go"` with its Go version. All other
fields are optional and informational.

### 1.6 Example

A minimal command envelope, taken verbatim from
[`tests/Conveyo.RabbitMQ.Test/GoldenEnvelopes/plain-command.json`](../tests/Conveyo.RabbitMQ.Test/GoldenEnvelopes/plain-command.json):

```json
{
  "envelopeVersion": "1",
  "messageId": "11111111-2222-3333-4444-555555555555",
  "correlationId": "33333333-4444-5555-6666-777777777777",
  "destinationAddress": "queue:golden-destination",
  "messageType": [
    "conveyo:golden.submit-invoice.v1"
  ],
  "message": {
    "invoiceNumber": "INV-2026-0001",
    "retryCount": 3,
    "force": true
  },
  "sentTime": "2026-05-14T12:34:56.789Z",
  "headers": null,
  "host": {
    "machineName": "golden-host",
    "processName": "Conveyo.GoldenTests",
    "processId": 4242,
    "assembly": "Conveyo.Test",
    "conveyoVersion": "0.0.0-golden",
    "operatingSystemVersion": "Unix 6.1.0",
    "runtime": "dotnet",
    "runtimeVersion": "10.0.0"
  }
}
```

Further examples (events with nested lists, multi-URN `messageType`,
`headers`, and `MessageData` payloads) live in
[`tests/Conveyo.RabbitMQ.Test/GoldenEnvelopes/`](../tests/Conveyo.RabbitMQ.Test/GoldenEnvelopes/).

### 1.7 AMQP basic properties

In addition to the envelope body, Conveyo sets these AMQP properties on
every publish:

| Property        | Value |
| --------------- | ----- |
| `content-type`  | `application/json` |
| `delivery-mode` | `2` (persistent) |
| `message-id`    | `envelope.messageId` (string form), if set |
| `correlation-id`| `envelope.correlationId` (string form), if set |
| `type`          | `envelope.messageType[0]` (the primary URN) |
| `timestamp`     | `envelope.sentTime` as Unix seconds, if set |
| `headers`       | `{ "conveyo-version": envelope.envelopeVersion }` |

A cross-language sender SHOULD set the same properties. Conveyo consumers
read all routing decisions from the envelope body, not from AMQP
properties — the properties are informational.

## 2. Topology

Conveyo declares topology eagerly during `StartAsync` (both producer and
consumer processes). All declarations are idempotent — running them on a
broker where they already exist with the same arguments is a no-op.

### 2.1 Per-queue layout

For each consumed queue `<q>`:

```
                                    ┌────────────────────┐
                                    │ exchange <urn>     │ fanout
                                    │ (per messageType)  │
                                    └─────────┬──────────┘
                                              │ exchange-to-exchange bind
                                              ▼
                                    ┌────────────────────┐
publish(messageType, body) ───────► │ exchange <q>       │ fanout
                                    └─────────┬──────────┘
                                              │ queue bind
                                              ▼
                                    ┌────────────────────┐
                                    │ queue <q>          │
                                    └─────────┬──────────┘
                                              │
            ┌─────────────────────────────────┼─────────────────────────────────┐
            │ retries exhausted /             │                  no consumer    │
            │ envelope deserialize failed     │                  registered     │
            ▼                                 │                                 ▼
direct-publish (no exchange) ─────► ┌────────────────────┐    direct-publish (no exchange) ────► ┌────────────────┐
                                    │ queue <q>_error    │                                       │ queue <q>_skipped│
                                    └────────────────────┘                                       └────────────────┘
            carries headers:                                  carries headers:
              conveyo-outcome = "faulted"                       conveyo-outcome = "skipped"
              conveyo-fault-original-queue = <q>                conveyo-skipped-reason = <message>
              conveyo-fault-reason = "exception" |              conveyo-skipped-original-queue = <q>
                                  "deserialization-failed" |
                                  "envelope-too-large"
              conveyo-fault-exception-type = <type FullName>
              conveyo-fault-exception-message = "Exception details redacted."
              conveyo-fault-attempts = <int as string>
              conveyo-fault-timestamp = <RFC 3339 UTC>
```

### 2.2 Naming convention

| Object                  | Name                  | Notes |
| ----------------------- | --------------------- | ----- |
| Main exchange           | `<queueName>`         | fanout, durable, not auto-delete |
| Main queue              | `<queueName>`         | durable; no `x-dead-letter-exchange` argument |
| Per-message exchange    | `<urn>` (e.g. `weather:WeatherObservationRecordedEvent.v2`) | fanout; bound *to* the main exchange via exchange-to-exchange binding |
| Error queue             | `<queueName>_error`   | durable; declared lazily on first failed message; Conveyo direct-publishes here with `conveyo-fault-*` discriminator headers. No exchange or broker DLX involvement. |
| Skipped queue           | `<queueName>_skipped` | durable; declared lazily on first skipped message; Conveyo direct-publishes here with `conveyo-skipped-*` discriminator headers. |

All exchanges are `fanout`, `durable=true`, `autoDelete=false`. All
queues are `durable=true`, `exclusive=false`, `autoDelete=false`.

Conveyo does **not** use RabbitMQ's `x-dead-letter-exchange` queue
argument. The error queue is fed by pipeline publish from the consumer
process, which lets Conveyo attach fault discriminator headers on the
dead-lettered copy. Cross-language consumers that bind to
`<queueName>_error` should read the `conveyo-fault-*` headers rather
than expecting broker-set `x-death`.

Exception messages and stack traces are not published into broker-visible
fault headers. `conveyo-fault-exception-type` identifies the top-level
exception type; `conveyo-fault-exception-message` is intentionally redacted.
By default the inner-exception chain is also dropped from broker-visible
fault metadata and from `Fault<T>` payloads — only the outermost exception
type is surfaced. Full exception details remain available to in-process
logging and fault hooks. For local development/debugging, .NET callers can
opt in with `IncludeFaultExceptionDetails()` on `IConveyoBuilder` for
`Fault<T>` payloads (which then includes inner exceptions and stack traces)
and on `RabbitMqHostOptions` for RabbitMQ `_error` queue headers.

### 2.3 Producer-side declarations

A producer process declares every URN exchange listed via `cfg.Map<T>(...)`
at startup, so it can publish even before any consumer is up. It does
**not** declare consumer queues or the error/skipped queues — those are
owned by the consumer process. The consumer process declares `_error` and
`_skipped` queues lazily, just before the first terminal publish to each
queue.

Exchanges for `Fault<T>` URNs (any URN whose registered type is
`Fault<>`) are an exception: they are declared **lazily, on the first
publish of that fault type**, not at startup. Most fault exchanges have
no subscribers in practice and eager declaration clutters the broker.
Lazy declaration is per-process-lifetime cached, so the round-trip is
paid once.

### 2.4 Routing on publish vs. send

| Operation       | Exchange         | Routing key | Mandatory flag | Behavior if no queue is bound |
| --------------- | ---------------- | ----------- | -------------- | ----------------------------- |
| `Publish<T>`    | `<primary urn>`  | (ignored — fanout) | `false` | Silently dropped. |
| `Send<T>`       | (empty / default) | `<queueName>` | `true` | Broker returns the message; Conveyo throws `UnroutableMessage`. |

### 2.5 Failure paths

All three failure paths use the same mechanism: Conveyo declares the
sibling queue (`<queueName>_error` or `<queueName>_skipped`) if this
process has not already done so, then the original message body is
direct-published via the default (empty) exchange with `mandatory=true`.
After the terminal copy is accepted, the original delivery is ack-ed off
the main queue. Conveyo never `BasicNack`-s; broker dead-letter is not
used.

- A consumer that throws is retried up to `MaxRetryCount` times with
  exponential delays (1s, 2s, 4s, …). On final failure the message is
  published to `<queueName>_error` with `conveyo-fault-reason =
  "exception"` and the last exception captured in
  `conveyo-fault-exception-*` headers.
- A malformed envelope (invalid JSON, missing required fields, or a JSON
  body of `null`) is published to `<queueName>_error` with
  `conveyo-fault-reason = "deserialization-failed"` and the parser
  exception captured in `conveyo-fault-exception-*` headers.
- An envelope whose body exceeds the configured `MaxEnvelopeSizeBytes`
  limit is published to `<queueName>_error` with `conveyo-fault-reason =
  "envelope-too-large"`. The original body is **not** propagated — the
  published message has an empty body and carries only the
  `conveyo-fault-*` discriminator headers. Cross-language `_error`
  subscribers must check `conveyo-fault-reason` before attempting to
  parse the body.
- A consumer that throws `MessageNotConsumedException` (no handler
  registered for the message type) is published to
  `<queueName>_skipped` with the `conveyo-skipped-*` headers shown
  above.

In every case except `envelope-too-large` the original message body is
preserved unchanged; the fault/skip metadata lives in the AMQP `headers`
table on the published copy.

`Fault<T>` events are emitted independently when a consumer exception is
routed to the error queue. They are *additional* signal for reactive
subscribers (sagas, alerting); the authoritative record of the failure
is the message in `<queueName>_error`.

Faults are **published** to the URN exchange `<original-urn>.fault`
(fanout). Subscribers who want to observe faults bind their queue to this
exchange. Conveyo does not inspect inbound envelope headers when routing
faults — applications that need direct fault replies must apply their own
trusted routing policy rather than trusting an address carried on the
failed envelope.

## 3. MessageData

`MessageData<T>` carries an out-of-band payload by URI reference inside
the message. The supported URI schemes and their grammars are documented
in [`docs/messagedata-uris.md`](./messagedata-uris.md). At a glance:

| Backend                | Scheme    | Canonical form                                  |
| ---------------------- | --------- | ----------------------------------------------- |
| Inline base64 payload  | `data`    | `data:[<mediatype>];base64,<payload>`           |
| Postgres bytea chunks  | `pgbin`   | `pgbin://<schema>/files/<uuid>`                 |

On the wire, a `MessageData<T>` property is serialized as a single-key
object:

```json
"payload": { "address": "pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111" }
```

A consumer in any language resolves `address` against the appropriate
backend only when the locator's namespace matches the resolver's configured
storage namespace. There is no neutral bridge scheme or HTTP indirection. The
`data:` scheme is the one exception — Conveyo accepts base64-encoded inline
payloads rather than addressing a remote stream, so there is no remote resolver
to contact.

Consumers enforce a maximum hydrated MessageData payload size. The .NET
default is 64 MiB and can be changed with `MaxMessageDataBytes(...)`.
`MessageData<string>` and `MessageData<byte[]>` fail during hydration when the
limit is exceeded; `MessageData<Stream>` surfaces the same failure if the
consumer reads beyond the configured limit.

## 4. Versioning

`envelopeVersion` is currently `"1"` and applies to the **envelope
shape**, not to individual message payloads. The string form leaves room
for non-numeric tags (e.g. `"1-rc"`) without changing the schema.

Rules for consumers:

- A consumer MUST refuse envelopes whose `envelopeVersion` is unknown to
  it — i.e. anything other than `"1"` until a future version is defined.
  Routing such a message into the user's handler is incorrect.
- Compatible additive changes (adding optional fields) do **not** bump
  `envelopeVersion`. Cross-language clients SHOULD ignore unknown fields
  on read.
- Breaking changes (renaming a field, changing a field's type, changing
  required-ness) MUST bump `envelopeVersion`. The new version will be
  documented here before any code that emits it ships.

Payload (`message`) compatibility is the responsibility of the URN. Each
URN identifies one schema; if the schema changes incompatibly, mint a
new URN (e.g. `weather:WeatherObservationRecordedEvent.v2` alongside
`.v1`) rather than mutating the existing one.
