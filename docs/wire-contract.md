# Conveyo wire contract

Conveyo messages use a JSON envelope over RabbitMQ. Clients in any language
must follow the envelope, routing, and storage rules below to interoperate.

## 1. Envelope

### 1.1 Encoding

- Every message body is a single JSON object — the **envelope**.
- Encoding is UTF-8. No BOM. Producers use compact JSON; consumers accept JSON whitespace.
- Property order, equivalent escaping, and equivalent numeric formatting do not
  change a JSON value. Array order is significant.
- Property names are camelCase. Property reads are case-insensitive on the
  .NET side, but new senders MUST emit camelCase.
- Producers emit null-valued optional fields (e.g. `"headers": null`); receivers
  also accept omitted optional fields. Unknown envelope and host fields are ignored.

### 1.2 Fields

| Field                | JSON type           | Required | Semantics |
| -------------------- | ------------------- | -------- | --------- |
| `envelopeVersion`    | string              | Yes      | `"1"`. See [§4 Versioning](#4-versioning). |
| `messageId`          | string (UUID) \| null | No     | Fresh UUID for each outgoing message; copied to AMQP `message-id`. |
| `correlationId`      | string (UUID) \| null | No     | Default propagation key carried with the message across produce/consume. |
| `destinationAddress` | string (URI) \| null  | No     | Absolute URI. Replaced with `queue:<queueName>` on receive; producers MAY leave it null. |
| `messageType`        | array of string     | Yes      | One or more URNs identifying the message type. Most-specific first; see [§1.4](#14-messagetype-urns). |
| `message`            | object              | Yes      | The user payload. Shape is defined per `messageType`. |
| `sentTime`           | string (RFC 3339) \| null | No | When the producer serialized the envelope, UTC. |
| `headers`            | object (string→string) \| null | No | Application-defined string headers. Distinct from AMQP headers. |
| `host`               | object \| null      | No       | Producer host info; see [§1.5](#15-host). |

### 1.3 Timestamps

`sentTime` is an RFC 3339 / ISO 8601 string, always in UTC with a `Z`
suffix. The .NET serializer omits trailing zero fractional seconds
(e.g. `2026-05-14T12:34:56.789Z`, not `2026-05-14T12:34:56.7890000Z`).
Use uppercase `T` and `Z` and include seconds. The accepted grammar is
`YYYY-MM-DDTHH:mm:ss[.fraction]Z`, with 1–16 digits when a fraction is present.
Dates must be valid calendar dates. Reject offsets, missing zones, lowercase
separators, trailing whitespace, and more than sixteen fractional digits.
Missing or null timestamps are accepted.

.NET retains at most seven fractional digits and truncates later digits. Use at
most seven digits for timestamps that must round-trip losslessly through .NET.
These rules also apply to fault timestamps.

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

Host properties are optional strings or null: `machineName`, `processName`,
`conveyoVersion`, `operatingSystemVersion`, `runtime`, and `runtimeVersion`.
`runtime` and `runtimeVersion` describe the language runtime and its version.
All fields are informational; unknown host properties are accepted. See the host
object in the [command example](../contracts/fixtures/envelopes/command.json).

### 1.6 Shared examples

The [command envelope](../contracts/fixtures/envelopes/command.json) contains
identifiers, a destination, application headers, and a nested object/list payload.
Two further examples carry a MessageData reference (see [§3](#3-messagedata)):
[inline data](../contracts/fixtures/envelopes/message-data-inline.json) and
[a Postgres locator](../contracts/fixtures/envelopes/message-data-pgbin.json).
Tests use these same [protocol fixtures](testing.md#protocol-fixtures).

### 1.7 AMQP basic properties

Conveyo sets these AMQP properties when publishing an envelope:

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

Producer and consumer processes declare their main topology before sending or
receiving messages. Repeating a declaration with the same arguments has no effect.

### 2.1 Per-queue layout

Bind each consumed URN exchange to the queue exchange, then bind the queue to
its queue exchange:

```text
publish → <urn> exchange → <queueName> exchange → <queueName> queue
send    → default exchange → <queueName> queue
failure → default exchange → <queueName>_error or <queueName>_skipped
```

### 2.2 Naming convention

| Object                  | Name                  | Notes |
| ----------------------- | --------------------- | ----- |
| Main exchange | `<queueName>` | Receives events from bound URN exchanges. |
| Main queue | `<queueName>` | Receives commands and events. |
| Per-message exchange | `<urn>` | One exchange per mapped message type. |
| Error queue | `<queueName>_error` | Receives failed deliveries. |
| Skipped queue | `<queueName>_skipped` | Receives unhandled deliveries. |

All exchanges are `fanout`, `durable=true`, `autoDelete=false`. All
queues are `durable=true`, `exclusive=false`, `autoDelete=false`.

### 2.3 Producer-side declarations

A producer declares mapped application URN exchanges at startup, so it can
publish before a consumer is up. Consumers own their receive queues, queue
exchanges, and bindings.

Fault URN exchanges (`<original-urn>.fault`) and terminal queues are redeclared
before each publication. The RabbitMQ client automatically recovers startup
topology after a lost connection.

### 2.4 Routing on publish vs. send

| Operation       | Exchange         | Routing key | Mandatory flag | Behavior if no queue is bound |
| --------------- | ---------------- | ----------- | -------------- | ----------------------------- |
| `Publish<T>`    | `<primary urn>`  | (ignored — fanout) | `false` | Silently dropped. |
| `Send<T>`       | (empty / default) | `<queueName>` | `true` | Broker returns the message; Conveyo reports an unroutable error (.NET: `UnroutableMessageException`). |

### 2.5 Failure paths

Conveyo declares the sibling terminal queue, publishes through the default
exchange with `mandatory=true`, then acknowledges the original only after the
terminal copy is confirmed and not returned.

| Failure | Queue | `conveyo-fault-reason` | Body |
| --- | --- | --- | --- |
| Dispatch retry exhaustion | `<queueName>_error` | `exception` | Original bytes. |
| Invalid envelope, including JSON `null` or missing required fields | `<queueName>_error` | `deserialization-failed` | Original bytes. |
| Envelope exceeds the configured size limit | `<queueName>_error` | `envelope-too-large` | Empty. |
| No matching consumer or `MessageNotConsumedException` | `<queueName>_skipped` | Not set. | Original bytes. |

Malformed and oversized envelopes are not retried. Error headers describe the
last dispatch exception or the validation error. Check `conveyo-fault-reason`
before parsing an error body; oversized copies contain no JSON. All terminal
metadata is in AMQP headers, leaving preserved bodies unchanged.

| Terminal header | Value |
| --- | --- |
| `conveyo-outcome` | `faulted` or `skipped`. |
| `conveyo-fault-original-queue` / `conveyo-skipped-original-queue` | Original receiving queue. |
| `conveyo-fault-reason` | `exception`, `deserialization-failed`, or `envelope-too-large`. |
| `conveyo-fault-exception-type` | Top-level exception type name. |
| `conveyo-fault-exception-message` | `Exception details redacted.` by default. |
| `conveyo-fault-attempts` | Attempt count as a string. |
| `conveyo-fault-timestamp` | UTC timestamp. |
| `conveyo-skipped-reason` | Skip reason text. |

Faulted copies use `conveyo-fault-*`; skipped copies use `conveyo-skipped-*`.
Conveyo does not use `BasicNack` or `x-dead-letter-exchange`; read these headers
rather than expecting broker-set `x-death`. Exception details remain available
to in-process logging. The .NET diagnostic opt-in adds exception messages and
`conveyo-fault-stack-trace`; see [RabbitMQ](rabbitmq.md#fault-messages).

### 2.6 Confirms, retries, and lifecycle

Publish completion requires a positive publisher confirmation; a mandatory
return is a failure even if the broker also confirms. A confirm means broker
acceptance, not handler completion. A lost connection can leave the outcome
uncertain; at-least-once delivery requires handlers to tolerate duplicates.

If terminal publication fails, the original delivery remains unacknowledged.
The broker redelivers it when its consumer channel closes or the connection
recovers. The .NET transport propagates the failure and leaves the delivery
pending until that lifecycle event; it does not immediately recycle the channel.
Unknown messages and explicit skips are not retried. Shutdown cancellation does
not create a business fault.
The default is three retries after the initial attempt, delayed by 1s, 2s, and
4s, prefetch 16, and a 1 MiB inbound envelope limit. Each retry decodes a fresh
message and executes the full handler sequence again.

### 2.7 Fault payload

A fault is an ordinary envelope whose primary URN is `<original-urn>.fault`.
Its `message` is an object with these required fields:

| Field | JSON type | Meaning |
| --- | --- | --- |
| `faultId` | UUID string | Fresh identifier for the fault event |
| `faultedMessageId` | UUID string or null | Identifier of the original message |
| `timestamp` | UTC timestamp string | Time the fault was created |
| `exceptions` | Nonempty array of exception objects | Exceptions from the failed attempts |
| `host` | Object | Fault producer; uses the [host fields](#15-host) |
| `message` | Object | Original typed payload |

Each exception requires string `exceptionType` and `message` fields. Optional
`stackTrace` is a string or null; optional `innerException` is a recursive
exception object or null. Unknown fault and exception fields are accepted.
Diagnostic type names are language specific and must not be used as portable
identifiers.

By default, exception messages are `Exception details redacted.`, stack traces
and inner exceptions are null. See the [redacted example](../contracts/fixtures/payloads/fault.json).
Fault publication is best effort and precedes terminal routing after retry
exhaustion; a failed fault publish must not prevent the original reaching `_error`.
Malformed, oversized, and skipped messages do not produce a typed fault event.
The `_error` copy is the authoritative failure record. Subscribers bind to the
`<original-urn>.fault` exchange. Inbound envelope headers never select the fault
destination; direct replies require an application's trusted routing policy.

### 2.8 Metadata and tracing

Producers create a fresh message ID and UTC send time for every outgoing envelope.
Messages sent or published inside a consumer inherit its correlation ID and
application headers, including null correlation; correlation does not implicitly
fall back to the message ID. The receiving transport replaces destination metadata
with `queue:<receiving queue>` for dispatch. It does not trust an inbound destination
to choose another queue.

Application headers are string-valued JSON entries inside `headers`. They are
not copied into AMQP headers. The AMQP table contains protocol metadata such as
`conveyo-version`, terminal diagnostics, and W3C `traceparent` / `tracestate`.
Tracing fields travel as UTF-8 AMQP values; .NET also accepts string values on
input. Consumers extract the remote parent and producers inject the current
activity context. `tracestate` accompanies a valid `traceparent`; invalid trace
context starts a new trace. Terminal copies preserve original identity and trace
properties. See [AMQP expectations](../contracts/fixtures/transport/rabbitmq.json).

### 2.9 Application payload representation

Payload schemas belong to applications. Use concrete numeric types to preserve
integer and decimal precision. .NET defaults to numeric enums and base64 `byte[]`;
see the [numeric and binary fixture](../contracts/fixtures/payloads/types.json).

## 3. MessageData

MessageData carries a payload reference inside the message. Resolve inline
`data:` bytes or Postgres `pgbin://` locators according to
[MessageData URI schemes](messagedata-uris.md).

A non-null MessageData reference is a JSON object with a required `address`
string containing a nonempty absolute URI. Producers emit only this key; readers
accept unknown keys. Reject missing, null, empty, or relative addresses and a
bare JSON string in place of the reference object. A null MessageData property
is permitted. See the two [reference envelope examples](#16-shared-examples).
Reference deserialization checks shape and absolute URI syntax; resolution
separately validates a supported scheme and its locator rules.

Consumers enforce a maximum hydrated MessageData payload size. The default
decoded limit is 64 MiB. Materialized text and byte arrays fail
when that limit is exceeded; streaming reads fail when reading past the limit
instead of silently returning EOF. .NET configures this limit with
`MaxMessageDataBytes`; see the [MessageData guide](messagedata.md#limits).
Text uses UTF-8; binary data retains the decoded bytes.

## 4. Versioning

`envelopeVersion` versions the envelope shape. Consumers MUST reject unsupported
versions; only the string `"1"` is supported, not the number `1`.

Adding optional fields does not bump the version; readers SHOULD ignore unknown
fields. Renaming fields, changing types, or changing whether a field is required
MUST bump the version.

Payload compatibility belongs to the URN. Use a new URN for an incompatible
payload change, such as `example:observation.v2` alongside `.v1`.
