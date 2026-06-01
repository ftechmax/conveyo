# MessageData URI Schemes

`MessageData<T>` carries an out-of-band payload by reference. The reference is a
URI written into the message envelope; consumers in any language must be able to
resolve that URI back to the original byte stream by looking at the scheme and
following the storage-specific contract for that backend.

Each storage backend has **exactly one** canonical scheme. There is no neutral
bridge scheme, no `https://` indirection, and no fallback chain — if a payload
was written via Postgres it can only be read back via Postgres.

| Backend                | Scheme    | Canonical form                                  |
| ---------------------- | --------- | ----------------------------------------------- |
| Inline base64 payload  | `data`    | `data:[<mediatype>];base64,<payload>`           |
| Postgres bytea chunks  | `pgbin`   | `pgbin://<schema>/files/<uuid>`                 |

A client SHOULD reject any other scheme that purports to address a
`MessageData` payload. A repository implementation MUST refuse to emit or
resolve any URI that does not match its canonical shape or configured storage
namespace.

---

## `data:` — Inline base64 payload

The `data:` scheme is the one exception to the "one scheme per backend" rule:
Conveyo uses it to carry bytes **directly inside the URI** rather than
addressing a remote byte stream. It is the canonical form for small payloads
that a producer chooses to inline instead of round-tripping through Postgres.

### Grammar

Conveyo supports only base64-encoded inline payloads:

```
data-uri  = "data:" [ mediatype ] ";base64" "," payload
mediatype = type "/" subtype *( ";" parameter )
payload   = *base64-char            ; base64 encoding of the raw bytes
```

`mediatype` is informational and MAY be omitted. The `base64` marker is a
metadata parameter and MUST appear as a complete parameter name,
case-insensitively. Producers MUST NOT emit percent-encoded inline payloads;
consumers MUST reject them.

### Semantics

- The URI **is** the payload. There is no remote resolver to contact, no
  authority, no scheme-specific table or bucket.
- A consumer base64-decodes the payload and surfaces the bytes exactly as it
  would for a remote-scheme resolver.
- The producer is responsible for keeping inline payloads small enough to fit
  comfortably in the message envelope — `data:` is not a general-purpose
  replacement for `pgbin://`.

### Validation rules

A resolver MUST:

1. Verify the scheme is `data` (case-insensitive).
2. Require a complete `base64` metadata parameter before the comma; reject
   percent-encoded and otherwise non-base64 inline payloads.
3. Base64-decode the payload and reject malformed base64.
4. Treat the decoded bytes as the payload — never attempt to dereference the
   URI against a remote backend.

### Why it is not a "parallel address"

The "one scheme per backend" rule prevents two URIs from addressing the same
byte stream and drifting out of sync. `data:` does not address a byte stream
at all — it *carries* the bytes — so it cannot drift against anything. It is
listed here so that consumers know to accept it on the read path, not because
it competes with the remote schemes.

---

## `pgbin://` — Postgres `bytea` chunks

### Grammar

```
pgbin-uri     = "pgbin://" schema "/" files-segment "/" file-id
schema        = 1*( ALPHA / DIGIT / "_" )    ; valid SQL identifier
files-segment = "files"
file-id       = UUID                          ; RFC 4122 textual form
```

`schema` is the URI authority. `files-segment` and `file-id` are the two path
segments. The canonical value of `files-segment` is the literal string
`files`; resolvers MUST NOT use this segment to choose a different table.
No query string, fragment, userinfo, or port is permitted.

### Semantics

- `schema` is the Postgres schema (namespace) that holds the `files` and
  `chunks` tables. Only ASCII letters, digits, and underscore are permitted;
  resolvers MUST refuse a URI whose schema contains any other character so
  that the value can be safely interpolated into SQL identifiers.
- `file-id` is the primary key of the row in `{schema}.files`. It is a UUID
  in standard 8-4-4-4-12 hex form.

### Storage layout

The backend owns two tables in `{schema}`:

#### `{schema}.files`

| Column         | Type           | Notes                                       |
| -------------- | -------------- | ------------------------------------------- |
| `id`           | `uuid`         | Primary key. Matches `file-id` in the URI.  |
| `created_at`   | `timestamptz`  | Server timestamp at insert (`now()`).       |
| `expire_at`    | `timestamptz`  | Nullable. Producer-supplied TTL deadline.   |
| `content_type` | `text`         | Reserved. Currently always NULL.            |
| `encoding`     | `text`         | `gzip` or NULL. See below.                  |
| `length`       | `bigint`       | Total bytes of the decoded payload.         |
| `chunk_size`   | `integer`      | Chunk size used when the file was written.  |
| `sha256`       | `text`         | Lowercase hex of SHA-256 over the **plain** payload (before gzip). |

#### `{schema}.chunks`

| Column    | Type      | Notes                                            |
| --------- | --------- | ------------------------------------------------ |
| `file_id` | `uuid`    | FK to `{schema}.files(id)` ON DELETE CASCADE.    |
| `n`       | `integer` | Chunk ordinal, starting at 0.                    |
| `data`    | `bytea`   | Chunk bytes.                                     |

The primary key is `(file_id, n)`. Chunks form a strict total order by `n`;
concatenating them in ascending order reproduces the on-disk byte stream of
the (possibly gzipped) payload.

### Validation rules

A resolver MUST:

1. Verify the scheme is `pgbin` (case-insensitive).
2. Reject the URI if `schema` is empty or contains any character outside
   `[A-Za-z0-9_]`.
3. Reject the URI if the path has fewer or more than two segments after the
   authority.
4. Reject the URI if `files-segment` is not the literal string `files`.
5. Reject the URI if `file-id` does not parse as a UUID.
6. Reject the URI if `schema` does not match the resolver's configured schema.

### Driver behaviour

To read a payload:

1. Verify the authority matches the resolver's configured schema and the path
   uses the canonical `files` segment.
2. Open a connection to the configured Postgres instance.
3. Look up `encoding` for the file:

   ```sql
   SELECT encoding FROM "<schema>".files WHERE id = $1;
   ```

   If the row does not exist, surface a "not found" error to the caller (in
   .NET: `FileNotFoundException`). Do not fall back to any other scheme or
   location.
4. Stream chunks in order:

   ```sql
   SELECT data FROM "<schema>".chunks WHERE file_id = $1 ORDER BY n;
   ```

   Use a streaming/sequential-access cursor — payloads can be large and
   should not be fully buffered.
4. If `encoding` is `gzip` (case-insensitive), wrap the concatenated chunk
   stream in a gzip decoder before returning it. Otherwise return the bytes
   verbatim. The returned stream is the **decoded** payload; the `length`
   column refers to that decoded byte count.
5. Disposing the returned stream MUST close the underlying database cursor
   and connection.

### Expiry

`expire_at` is informational on the read path; expiry is enforced by a
background sweep (e.g. a `DELETE FROM {schema}.files WHERE expire_at < now()`
job) rather than per-read. Consumers MUST treat a missing row identically to
"expired and swept".

### Examples

```
pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111
pgbin://message_data/files/9e1c4f06-0a3b-4d5d-9ad4-2b3c4d5e6f70
```

---

## Why no neutral or HTTP scheme

A single canonical scheme per backend keeps the wire contract minimal and
language-agnostic:

- A Go, Rust, or Python consumer can look at the URI scheme and immediately
  pick the right driver without consulting a registry or a server.
- Producers and consumers cannot drift — there is no parallel address for the
  same payload, so there is no scheme to keep in sync, and no fallback chain
  that can silently mask a misconfiguration.
- Storage repositories own emission and resolution end-to-end; they refuse
  any other scheme on input and never emit any other scheme on output.

Introducing a neutral bridge scheme (e.g. `conveyo-data://`) or HTTP
indirection would re-add the indirection cost without buying portability, and
it would create two ways to address the same byte stream — exactly the
ambiguity this contract is designed to prevent.
