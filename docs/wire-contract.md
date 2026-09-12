# Conveyo wire contract

Conveyo messages use a JSON envelope over RabbitMQ. Clients in any language
must follow the envelope, routing, and storage rules below to interoperate.

The [shared contract suite](../contracts/README.md) combines JSON Schema,
written specifications, and fixtures. Resolve disagreements between these
contracts and implementations together.

## 1. Envelope

### 1.1 Encoding

- Every message body is a single JSON object — the **envelope**.
- Encoding is UTF-8. No BOM. Producers use compact JSON; consumers accept JSON whitespace.
- Compare JSON semantically: property order, equivalent escaping, and equivalent
  numeric formatting are not compatibility requirements. Array order is significant.
- Property names are camelCase. Property reads are case-insensitive on the
  .NET side, but new senders MUST emit camelCase.
- Null-valued fields are emitted, not omitted (e.g. `"headers": null`).
- The AMQP `content-type` is `application/json`. Messages are published with
  `delivery-mode = 2` (persistent).

### 1.2 Fields

| Field                | JSON type           | Required | Semantics |
| -------------------- | ------------------- | -------- | --------- |
| `envelopeVersion`    | string              | Yes      | Currently `"1"`. See [§4 Versioning](#4-versioning). |
| `messageId`          | string (UUID) \| null | No     | Fresh UUID for each outgoing message; copied to AMQP `message-id`. |
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
Use uppercase `T` and `Z`, include seconds, and use at most seven fractional
digits for lossless .NET `DateTime` precision. The shared timestamp schemas accept
up to sixteen fractional digits, matching the .NET parser; digits beyond the
seventh are truncated by .NET. More than sixteen fractional digits are rejected.
Go's `time.Parse(time.RFC3339Nano, value)` and `time.Time.UnmarshalJSON` parse
these timestamps; Go retains up to nine fractional digits. Use at most seven
digits when the timestamp must round-trip through both languages without losing
precision. Go receivers must also enforce the shared UTC and precision limits:
the Go parser alone accepts offsets and more than sixteen fractional digits.
Missing or null timestamps are accepted. Non-UTC timestamps are rejected.
UUIDs use the standard hyphenated 8-4-4-4-12 hexadecimal form; letter case is
insignificant. No specific UUID version is required.

### 1.4 `messageType` URNs

- The array MUST contain at least one entry.
- Entry 0 is the **primary URN** — the one this envelope is published with
  on the wire and the name of the corresponding RabbitMQ exchange.
- Additional entries provide ordered dispatch fallback **after delivery**. The
  receiver chooses the first locally registered URN, then finds handlers for the
  receiving endpoint. An entry does not create a broker route or binding; a
  subscriber to a secondary URN needs an explicit route that delivers the message.
- Normal library publishing emits one primary URN.
- URNs are case-sensitive, nonempty strings containing only ASCII letters,
  digits, `.`, `_`, `:`, and `-`, with at most 255 bytes (AMQP short-string limit).
  There is no required URI scheme. Derived fault URNs append `.fault` and must
  satisfy the same limit, leaving at most 249 bytes for a normal mapped URN.

### 1.5 `host`

```jsonc
{
  "machineName": "golden-host",
  "processName": "Conveyo.GoldenTests",
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

A command envelope from
[`contracts/fixtures/envelopes/plain-command.json`](../contracts/fixtures/envelopes/plain-command.json):

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
    "conveyoVersion": "0.0.0-golden",
    "operatingSystemVersion": "Unix 6.1.0",
    "runtime": "dotnet",
    "runtimeVersion": "10.0.0"
  }
}
```

Further examples (events with nested lists, multi-URN `messageType`,
`headers`, and `MessageData` payloads) live in
[`contracts/fixtures/envelopes/`](../contracts/fixtures/envelopes/).

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

Conveyo declares the main topology during `StartAsync` in both producer and
consumer processes. Repeating a declaration with the same arguments has no effect.

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
publish of that fault type**, not at startup. This avoids creating unused fault
exchanges.
The .NET declaration cache is per process. Recovery must preserve or recreate
required broker topology before traffic resumes.

### 2.4 Routing on publish vs. send

| Operation       | Exchange         | Routing key | Mandatory flag | Behavior if no queue is bound |
| --------------- | ---------------- | ----------- | -------------- | ----------------------------- |
| `Publish<T>`    | `<primary urn>`  | (ignored — fanout) | `false` | Silently dropped. |
| `Send<T>`       | (empty / default) | `<queueName>` | `true` | Broker returns the message; Conveyo reports an unroutable error (.NET: `UnroutableMessageException`). |

### 2.5 Failure paths

All failure paths use the same routing mechanism: Conveyo declares the
sibling queue (`<queueName>_error` or `<queueName>_skipped`) if this
process has not already done so, then the original message body is
direct-published via the default (empty) exchange with `mandatory=true`.
Conveyo acknowledges the original delivery only after the terminal copy is
confirmed and not returned. It does not call `BasicNack` or use broker dead-lettering.

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
routed to the error queue. Subscribers can use them for sagas or alerting.
The message in `<queueName>_error` is the authoritative failure record.

Faults are **published** to the URN exchange `<original-urn>.fault`
(fanout). Subscribers who want to observe faults bind their queue to this
exchange. Conveyo does not inspect inbound envelope headers when routing
faults — applications that need direct fault replies must apply their own
trusted routing policy rather than trusting an address carried on the
failed envelope.

### 2.6 Confirms, retries, and lifecycle

Publish completion requires a positive publisher confirmation; a mandatory
return is a failure even if the broker also confirms. A confirm means broker
acceptance, not handler completion. A lost connection can leave the outcome
uncertain; at-least-once delivery requires handlers to tolerate duplicates.

If terminal publication fails, the consumer channel is recycled so the original
unacknowledged delivery can be redelivered. Unknown messages and explicit skips
are not retried. Shutdown cancellation does not create a business fault.
The default is three retries after the initial attempt, delayed by 1s, 2s, and
4s, prefetch 16, and a 1 MiB inbound envelope limit. .NET retries decode a fresh
message and execute the full handler sequence again.

.NET hosting starts the configured transport before consuming and closes it on
stop. Language-specific lifecycle APIs and concurrency controls belong in usage
guides, rather than in the serialized envelope contract.

### 2.7 Fault payload

A fault is an ordinary envelope whose primary URN is `<original-urn>.fault`.
Its `message` has the structure in [fault.schema.json](../contracts/schemas/fault.schema.json):
`faultId` (fresh UUID), `faultedMessageId` (original UUID or null), `timestamp`
(UTC), `exceptions` (array), `host` (fault producer), and `message` (original typed
payload). Each exception has `exceptionType`, `message`, nullable `stackTrace`,
and nullable recursive `innerException`. Diagnostic type names are language
specific and must not be used as portable identifiers.

By default, exception messages are `Exception details redacted.`, stack traces
and inner exceptions are null. See the [redacted fixture](../contracts/fixtures/payloads/fault.json).
Fault publication is best effort and precedes terminal routing after retry
exhaustion; a failed fault publish must not prevent the original reaching `_error`.
Malformed, oversized, and skipped messages do not produce a typed fault event.

### 2.8 Metadata and tracing

.NET creates a fresh message ID and UTC send time for every outgoing envelope.
Messages sent or published inside a consumer inherit its correlation ID and
application headers, including null correlation; correlation does not implicitly
fall back to the message ID. The receiving transport replaces destination metadata
with `queue:<receiving queue>` for dispatch. It does not trust an inbound destination
to choose another queue. Host fields describe the producing process and are
informational; all fields may be absent or null. Unknown envelope and host fields
are accepted.

Application headers are string-valued JSON entries inside `headers`. They are
not copied into AMQP headers. The AMQP table contains protocol metadata such as
`conveyo-version`, terminal diagnostics, and W3C `traceparent` / `tracestate`.
Tracing fields travel as UTF-8 AMQP values; .NET also accepts string values on
input. Consumers extract the remote parent and producers inject the current
activity context. `tracestate` accompanies a valid `traceparent`; invalid trace
context starts a new trace. Terminal copies preserve original identity and trace
properties. See [AMQP expectations](../contracts/fixtures/transport/rabbitmq.json).

### 2.9 Application payload representation

Payload schemas belong to applications. The shared
[numeric and binary example](../contracts/fixtures/payloads/types.json) covers a
signed 64-bit integer, decimal, numeric enum, explicit null, ordered collection,
and base64 bytes. Its [.NET conformance test](../tests/Conveyo.RabbitMQ.Test/SharedContractTests.cs)
uses `long`, `decimal`, an enum, and `byte[]`. Other languages must use suitable
concrete numeric types to avoid losing integer or decimal precision. Enum names
versus numbers are an application contract choice (.NET defaults to numbers).

## 3. MessageData

`MessageData<T>` carries an out-of-band payload by URI reference inside
the message. The supported URI schemes and their grammars are documented
in [MessageData URI schemes](./messagedata-uris.md):

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
storage namespace. A `data:` URI contains base64-encoded inline bytes and needs
no remote resolver.

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
  whether a field is required) MUST bump `envelopeVersion`. The new version
  will be documented here before any code that emits it ships.

Payload (`message`) compatibility is the responsibility of the URN. Each
URN identifies one schema; if the schema changes incompatibly, use a
new URN (e.g. `weather:WeatherObservationRecordedEvent.v2` alongside
`.v1`) rather than mutating the existing one.
