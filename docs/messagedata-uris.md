# MessageData URI Schemes

`MessageData<T>` carries a URI in the message envelope. Its scheme tells the
consumer how to read the payload: decode inline bytes or load them from Postgres.
Each storage backend has one canonical scheme. Postgres payloads must be read
from Postgres; resolvers do not use HTTP indirection or fall back to another backend.

| Backend                | Scheme    | Canonical form                                  |
| ---------------------- | --------- | ----------------------------------------------- |
| Inline base64 payload  | `data`    | `data:[<mediatype>];base64,<payload>`           |
| Postgres bytea chunks  | `pgbin`   | `pgbin://<schema>/files/<uuid>`                 |

A client SHOULD reject other schemes for `MessageData` payloads. A repository
implementation MUST refuse to emit or resolve any URI that does not match its canonical shape or configured storage
namespace.

## `data:` — Inline base64 payload

The `data:` scheme carries base64-encoded bytes inside the URI. Use it for
small payloads that fit in the message envelope.

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

- The URI contains the payload and has no authority or remote storage location.
- A consumer base64-decodes the payload and surfaces the bytes exactly as it
  would for a remote-scheme resolver.
- The producer must keep inline payloads within the message envelope size limit.

### Validation rules

A resolver MUST:

1. Verify the scheme is `data` (case-insensitive).
2. Require a complete `base64` metadata parameter before the comma; reject
   percent-encoded and otherwise non-base64 inline payloads.
3. Base64-decode the payload and reject malformed base64.
4. Treat the decoded bytes as the payload — never attempt to dereference the
   URI against a remote backend.

## `pgbin://` — Postgres `bytea` chunks

### Grammar

```
pgbin-uri     = "pgbin://" schema "/" files-segment "/" file-id
schema        = 1*63( ALPHA / DIGIT / "_" ) ; ASCII, normalized to lowercase
files-segment = "files"
file-id       = UUID                          ; RFC 4122 textual form
```

`schema` is the URI authority. `files-segment` and `file-id` are the two path
segments. The canonical value of `files-segment` is the literal string
`files`; resolvers MUST NOT use this segment to choose a different table.
No query string (including an empty `?`), fragment, userinfo, or port is permitted.
Reject empty or extra segments, trailing slashes, dot segments, percent escapes,
and UUIDs in compact or braced form before URI normalization can hide them.
Scheme, authority, and UUID hex case are accepted case-insensitively; the bucket
name is exactly lowercase `files`. Producers emit lowercase scheme, schema, and UUID.

### Semantics

- `schema` is the Postgres schema (namespace) that holds the `files` and
  `chunks` tables. Only ASCII letters, digits, and underscore are permitted;
  resolvers MUST refuse a URI whose schema contains any other character so
  that the namespace is unambiguous. Configuration normalizes schema names to
  lowercase; an absent or blank .NET schema defaults to `md`. The limit is 63
  ASCII bytes, PostgreSQL's standard identifier limit. Quote schema and table
  identifiers consistently, including names starting with digits or SQL keywords.
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

The primary key is `(file_id, n)`. Concatenating chunks in ascending order of `n`
reproduces the stored byte stream, which may be gzip-compressed.

### Validation rules

A resolver MUST:

1. Verify the scheme is `pgbin` (case-insensitive).
2. Reject the URI if `schema` is empty or contains any character outside
   `[A-Za-z0-9_]`, or is longer than 63 bytes.
3. Reject the URI if the path has fewer or more than two segments after the
   authority.
4. Reject the URI if `files-segment` is not the literal string `files`.
5. Require `file-id` in exactly 8-4-4-4-12 hex form.
6. Reject the URI if `schema` does not match the resolver's configured schema.

### Driver behaviour

To read a payload:

1. Verify the authority matches the resolver's configured schema and the path
   uses the canonical `files` segment.
2. Open a connection to the configured Postgres instance.
3. Look up `encoding` for the file:

   ```sql
   SELECT encoding FROM "<schema>"."files"
   WHERE id = $1 AND (expire_at IS NULL OR expire_at > now());
   ```

   If the row does not exist or has expired, surface a "not found" error to the caller (in
   .NET: `FileNotFoundException`). Do not fall back to any other scheme or
   location.
4. Stream chunks in order:

   ```sql
   SELECT data FROM "<schema>"."chunks" WHERE file_id = $1 ORDER BY n;
   ```

   Use a streaming/sequential-access cursor — payloads can be large and
   should not be fully buffered.
5. If `encoding` is `gzip` (case-insensitive), wrap the concatenated chunk
   stream in a gzip decoder before returning it. Otherwise return the bytes
   verbatim. The returned stream is the **decoded** payload; the `length`
   column refers to that decoded byte count.
6. Disposing the returned stream MUST close the underlying database cursor
   and connection.

### Expiry

Rows with `expire_at <= now()` are unavailable immediately, even before cleanup.
Missing and expired rows both produce a not-found result. Cleanup deletes expired
`files` rows and cascades to `chunks`; it reclaims space and does not determine
read availability. The application owns cleanup scheduling.

### Writes and resource ownership

Write file metadata and all chunks in a single transaction; interrupted writes
roll back. `length` and lowercase SHA-256 describe the original decoded bytes,
including empty payloads. Compression is optional gzip over the complete byte
stream before chunking. Defaults are schema `md`, 1 MiB chunks, and gzip off;
.NET clamps configured chunks to 64 KiB–4 MiB. Chunks start at ordinal zero.
An empty uncompressed payload has no chunks; gzip still emits a valid gzip stream.

The caller owns the input stream. The caller must dispose an opened stream,
which releases its reader, command, and database connection. .NET consumer
hydration materializes bytes/text and owns cleanup for hydrated streams. The
planned Go API will require explicit reads; it is not implemented in Stage 1.
Both languages use a decoded limit of 64 MiB by default; a streaming read past
the limit must fail rather than silently return EOF. Inline reads use no repository.

### Examples

```
pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111
pgbin://message_data/files/9e1c4f06-0a3b-4d5d-9ad4-2b3c4d5e6f70
```
