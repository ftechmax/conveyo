# MessageData URI Schemes

`MessageData<T>` carries a URI in the message envelope. Its scheme tells the
consumer how to read the payload: decode inline bytes or load them from Postgres.
Postgres payloads are read from the configured database; locators identify a
schema and file, not a database connection.

| Backend                | Scheme    | Canonical form                                  |
| ---------------------- | --------- | ----------------------------------------------- |
| Inline base64 payload  | `data`    | `data:[<mediatype>];base64,<payload>`           |
| Postgres bytea chunks  | `pgbin`   | `pgbin://<schema>/files/<uuid>`                 |

The built-in readers support `data:` and `pgbin://`. The Postgres repository
rejects locators that do not match the grammar or its configured schema.

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
- Empty base64 payloads decode to zero bytes. Scheme and marker matching are
  case-insensitive; the media type does not change the bytes.
- A consumer base64-decodes the payload into bytes.
- The producer must keep inline payloads within the message envelope size limit.

Resolvers MUST reject malformed base64 and decode inline bytes without a remote lookup.

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

The authority MUST match the resolver's configured schema. Configuration
normalizes schemas to lowercase; the .NET default is `md`. Quote schema and table
identifiers, including names starting with digits or SQL keywords. `file-id`
identifies the row in `{schema}.files`.

### Storage layout

The backend owns two tables in `{schema}`:

#### `{schema}.files`

| Column         | Type           | Notes                                       |
| -------------- | -------------- | ------------------------------------------- |
| `id`           | `uuid`         | Primary key. Matches `file-id` in the URI.  |
| `created_at`   | `timestamptz`  | Server timestamp at insert (`now()`).       |
| `expire_at`    | `timestamptz`  | Nullable. Producer-supplied TTL deadline.   |
| `content_type` | `text`         | Written as NULL.                            |
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
read availability. .NET storage registration runs cleanup every five minutes by
default; applications can [configure or disable it](messagedata.md#register-a-repository).

### Writes and resource ownership

Write file metadata and all chunks in a single transaction; interrupted writes
roll back. `length` and lowercase SHA-256 describe the original decoded bytes,
including empty payloads. Compression is optional gzip over the complete byte
stream before chunking. Defaults are schema `md`, 1 MiB chunks, and gzip off;
.NET clamps configured chunks to 64 KiB–4 MiB. Chunks start at ordinal zero.
An empty uncompressed payload has no chunks; gzip still emits a valid gzip stream.

The caller owns the input stream; writes leave it open on success and failure.
The caller must dispose every returned read stream, including after a partial or
failed read, to release its reader, command, and database connection.

Consumer hydration and decoded size limits are covered in the
[MessageData guide](messagedata.md#consume-a-payload). Reference examples are
linked from the [wire contract](wire-contract.md#16-shared-examples).
