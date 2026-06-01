using System.Data;
using System.IO.Compression;
using System.Security.Cryptography;
using Npgsql;

namespace Conveyo.Storage.Postgres;

public sealed class PostgresMessageDataRepository : IMessageDataRepository
{
    public const string DefaultSchema = "md";
    public const int DefaultChunkSizeBytes = 1_048_576;

    private const string BucketName = "files";
    private const string GzipEncoding = "gzip";
    private const int MinChunkSizeBytes = 64 * 1024;
    private const int MaxChunkSizeBytes = 4 * 1024 * 1024;
    private const int CopyBufferSize = 1024 * 1024;
    private const string Scheme = "pgbin";

    private readonly string _connectionString;

    private readonly string _schema;

    private readonly int _chunkSize;

    private readonly bool _gzip;

    public PostgresMessageDataRepository(
        string connectionString,
        string schema = DefaultSchema,
        int chunkSizeBytes = DefaultChunkSizeBytes,
        bool gzip = false)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        _connectionString = connectionString;
        _schema = NormalizeSchema(schema);
        _chunkSize = Math.Clamp(chunkSizeBytes, MinChunkSizeBytes, MaxChunkSizeBytes);
        _gzip = gzip;
    }

    private static string NormalizeSchema(string schema)
    {
        var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? DefaultSchema : schema;
        if (!IsSafeIdentifier(effectiveSchema))
        {
            throw new ArgumentException(ErrorMessages.SchemaInvalidCharacters, nameof(schema));
        }

        // Lowercase to match the URI authority (Uri canonicalizes it) and unquoted SQL identifiers.
        return effectiveSchema.ToLowerInvariant();
    }

    private static bool IsSafeIdentifier(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        foreach (var c in value)
        {
            if (!char.IsLetterOrDigit(c) && c != '_')
            {
                return false;
            }
        }

        return true;
    }

    public async Task EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var sql = $"""

                   CREATE SCHEMA IF NOT EXISTS {_schema};

                   CREATE TABLE IF NOT EXISTS {_schema}.files (
                     id           uuid PRIMARY KEY,
                     created_at   timestamptz NOT NULL DEFAULT now(),
                     expire_at    timestamptz,
                     content_type text,
                     encoding     text,
                     length       bigint NOT NULL,
                     chunk_size   integer NOT NULL,
                     sha256       text
                   );

                   CREATE TABLE IF NOT EXISTS {_schema}.chunks (
                     file_id  uuid NOT NULL REFERENCES {_schema}.files(id) ON DELETE CASCADE,
                     n        integer NOT NULL,
                     data     bytea   NOT NULL,
                     PRIMARY KEY (file_id, n)
                   );

                   """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<Stream> GetAsync(Uri address, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(address);

        if (!string.Equals(address.Scheme, Scheme, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(ErrorMessages.UnsupportedUriScheme(address), nameof(address));
        }

        var (schema, bucket, id) = Parse(address);
        EnsureAddressMatchesConfiguredRepository(schema, bucket);

        var connection = new NpgsqlConnection(_connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);

            string? encoding;
            // Expired rows are treated as not-found, ahead of the cleanup sweep.
            await using (var headerCommand = new NpgsqlCommand(
                $"SELECT encoding FROM {schema}.files WHERE id=@id AND (expire_at IS NULL OR expire_at > now());", connection))
            {
                headerCommand.Parameters.AddWithValue("id", id);
                var scalar = await headerCommand.ExecuteScalarAsync(cancellationToken);
                if (scalar is null)
                {
                    throw new FileNotFoundException(ErrorMessages.MessageDataNotFound(id));
                }

                encoding = scalar == DBNull.Value ? null : (string?)scalar;
            }

            var chunkStream = new DbChunkSourceStream(connection, ChunksTable(schema), id);
            Stream payloadStream = string.Equals(encoding, GzipEncoding, StringComparison.OrdinalIgnoreCase)
                ? new GZipStream(chunkStream, CompressionMode.Decompress, leaveOpen: false)
                : chunkStream;

            // Disposing the returned stream cascades through to the reader, command, and connection.
            return payloadStream;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    public async Task<Uri> PutAsync(Stream data, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(data);

        var id = Guid.NewGuid();
        var expireAt = timeToLive.HasValue ? DateTime.UtcNow.Add(timeToLive.Value) : (DateTime?)null;

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var insertCommand = new NpgsqlCommand($@"
            INSERT INTO {_schema}.files(id, created_at, expire_at, content_type, encoding, length, chunk_size)
            VALUES (@id, now(), @exp, @ct, @enc, 0, @cs);", connection, transaction))
        {
            insertCommand.Parameters.AddWithValue("id", id);
            insertCommand.Parameters.AddWithValue("exp", (object?)expireAt ?? DBNull.Value);
            insertCommand.Parameters.AddWithValue("ct", DBNull.Value);
            insertCommand.Parameters.AddWithValue("enc", _gzip ? GzipEncoding : (object?)DBNull.Value!);
            insertCommand.Parameters.AddWithValue("cs", _chunkSize);
            await insertCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        using var sha256 = SHA256.Create();
        long plainLength = 0;

        await using (var sink = new DbChunkSinkStream(connection, transaction, ChunksTable(_schema), id, _chunkSize))
        {
            if (_gzip)
            {
                await using var gzipStream = new GZipStream(sink, CompressionMode.Compress, leaveOpen: true);
                plainLength = await CopyAndHashAsync(data, gzipStream, sha256, cancellationToken);
            }
            else
            {
                plainLength = await CopyAndHashAsync(data, sink, sha256, cancellationToken);
            }

            await sink.FlushAsync(cancellationToken);
            sha256.TransformFinalBlock([], 0, 0);

            var sha256Hex = Convert.ToHexString(sha256.Hash!).ToLowerInvariant();

            // length stores the decoded (plaintext) payload length per the wire-contract docs, even
            // when the rows are gzip-compressed. The Encoded byte total is implicit in the chunks.
            await using var updateCommand = new NpgsqlCommand($@"
                UPDATE {_schema}.files SET length=@len, sha256=@s WHERE id=@id;", connection, transaction);
            updateCommand.Parameters.AddWithValue("len", plainLength);
            updateCommand.Parameters.AddWithValue("s", sha256Hex);
            updateCommand.Parameters.AddWithValue("id", id);
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);

        return new Uri($"{Scheme}://{_schema}/{BucketName}/{id:D}");
    }

    /// <summary>
    /// Deletes expired files (chunks cascade via <c>ON DELETE CASCADE</c>); returns the count deleted.
    /// </summary>
    public async Task<int> DeleteExpiredAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(
            $"DELETE FROM {_schema}.files WHERE expire_at IS NOT NULL AND expire_at < now();", connection);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string ChunksTable(string schema) => $"{schema}.chunks";

    private static async Task<long> CopyAndHashAsync(
        Stream source,
        Stream destination,
        HashAlgorithm hash,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[CopyBufferSize];
        long length = 0;

        int read;
        while ((read = await source.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
        {
            hash.TransformBlock(buffer, 0, read, null, 0);
            length += read;
            await destination.WriteAsync(buffer, 0, read, cancellationToken);
        }

        return length;
    }

    private static (string schema, string bucket, Guid id) Parse(Uri uri)
    {
        if (!string.IsNullOrEmpty(uri.UserInfo) ||
            !uri.IsDefaultPort ||
            !string.IsNullOrEmpty(uri.Query) ||
            !string.IsNullOrEmpty(uri.Fragment))
        {
            throw new FormatException(ErrorMessages.InvalidLocator(uri));
        }

        var schema = uri.Host;
        if (!IsSafeIdentifier(schema))
        {
            throw new FormatException(ErrorMessages.InvalidLocatorUnsafeSchema(uri));
        }

        var segments = uri.AbsolutePath.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length != 2 || !Guid.TryParse(segments[1], out var id))
        {
            throw new FormatException(ErrorMessages.InvalidLocator(uri));
        }

        return (schema, segments[0], id);
    }

    private void EnsureAddressMatchesConfiguredRepository(string schema, string bucket)
    {
        if (!string.Equals(schema, _schema, StringComparison.Ordinal) ||
            !string.Equals(bucket, BucketName, StringComparison.Ordinal))
        {
            throw new UnauthorizedAccessException(
                ErrorMessages.LocatorTargetsDifferentRepository(schema, bucket, _schema));
        }
    }

    private sealed class DbChunkSinkStream : Stream
    {
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction _transaction;
        private readonly string _chunksTable;
        private readonly Guid _fileId;
        private readonly int _chunkSize;
        private readonly byte[] _buffer;
        private int _bufferedBytes;
        private int _chunkOrdinal;

        public long TotalWritten { get; private set; }

        public DbChunkSinkStream(NpgsqlConnection connection, NpgsqlTransaction transaction, string chunksTable, Guid fileId, int chunkSize)
        {
            _connection = connection;
            _transaction = transaction;
            _chunksTable = chunksTable;
            _fileId = fileId;
            _chunkSize = chunkSize;
            _buffer = new byte[_chunkSize];
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(buffer);
            return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            while (!buffer.IsEmpty)
            {
                var bytesToCopy = Math.Min(buffer.Length, _chunkSize - _bufferedBytes);
                buffer[..bytesToCopy].CopyTo(_buffer.AsMemory(_bufferedBytes));
                _bufferedBytes += bytesToCopy;
                buffer = buffer[bytesToCopy..];
                TotalWritten += bytesToCopy;

                if (_bufferedBytes == _chunkSize)
                {
                    await FlushChunkAsync(cancellationToken);
                }
            }
        }

        public override void Write(byte[] buffer, int offset, int count) =>
            WriteAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();

        public override Task FlushAsync(CancellationToken cancellationToken) =>
            _bufferedBytes > 0 ? FlushChunkAsync(cancellationToken) : Task.CompletedTask;

        public override void Flush() => FlushAsync(CancellationToken.None).GetAwaiter().GetResult();

        private async Task FlushChunkAsync(CancellationToken cancellationToken)
        {
            var chunk = _buffer.AsMemory(0, _bufferedBytes).ToArray();
            await using var command = new NpgsqlCommand($"INSERT INTO {_chunksTable}(file_id,n,data) VALUES (@id,@n,@d);", _connection, _transaction);
            command.Parameters.AddWithValue("id", _fileId);
            command.Parameters.AddWithValue("n", _chunkOrdinal++);
            command.Parameters.AddWithValue("d", chunk);
            await command.ExecuteNonQueryAsync(cancellationToken);
            _bufferedBytes = 0;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && _bufferedBytes > 0)
            {
                Flush();
            }
            base.Dispose(disposing);
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => TotalWritten;
        public override long Position { get => TotalWritten; set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }

    private sealed class DbChunkSourceStream : Stream
    {
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlCommand _command;
        private NpgsqlDataReader? _reader;
        private byte[]? _current;
        private int _offsetInChunk;

        public DbChunkSourceStream(NpgsqlConnection connection, string chunksTable, Guid fileId)
        {
            _connection = connection;
            _command = new NpgsqlCommand($@"SELECT data FROM {chunksTable} WHERE file_id=@id ORDER BY n;", _connection);
            _command.Parameters.AddWithValue("id", fileId);
        }

        public override async ValueTask DisposeAsync()
        {
            if (_reader is not null)
            {
                await _reader.DisposeAsync();
            }

            await _command.DisposeAsync();
            await _connection.DisposeAsync();
            await base.DisposeAsync();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _reader?.Dispose();
                _command.Dispose();
                _connection.Dispose();
            }
            base.Dispose(disposing);
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(buffer);
            return ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            _reader ??= await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

            var written = 0;
            while (!buffer.IsEmpty)
            {
                if (_current is null || _offsetInChunk >= _current.Length)
                {
                    if (!await _reader.ReadAsync(cancellationToken))
                    {
                        break;
                    }
                    _current = (byte[])_reader[0];
                    _offsetInChunk = 0;
                }

                var bytesToCopy = Math.Min(buffer.Length, _current.Length - _offsetInChunk);
                _current.AsMemory(_offsetInChunk, bytesToCopy).CopyTo(buffer);
                _offsetInChunk += bytesToCopy;
                buffer = buffer[bytesToCopy..];
                written += bytesToCopy;
            }

            return written;
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            ReadAsync(buffer, offset, count, CancellationToken.None).GetAwaiter().GetResult();

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
