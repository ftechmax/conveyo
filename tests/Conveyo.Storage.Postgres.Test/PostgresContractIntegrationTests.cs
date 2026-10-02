using System.IO.Compression;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Conveyo.Storage.Postgres.Test;

[TestFixture]
[Category("Integration")]
internal class PostgresContractIntegrationTests
{
    private const int ChunkSizeBytes = 65536;

    // Spans three chunks, with a final partial chunk.
    private const int MultiChunkPayloadLength = 150000;

    private PostgreSqlContainer? _container;
    private NpgsqlConnection? _connection;
    private CancellationTokenSource _timeout = null!;
    private string _configuredSchema = null!;

    private string Schema => _configuredSchema.ToLowerInvariant();

    private CancellationToken Token => _timeout.Token;

    [OneTimeSetUp]
    public async Task StartAsync()
    {
        var container = new PostgreSqlBuilder("postgres:18.6-alpine")
            .WithLabel(IntegrationContainers.RunLabels)
            .Build();
        await IntegrationContainers.StartAsync(container);
        _container = container;
    }

    [OneTimeTearDown]
    public async Task StopAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
            _container = null;
        }
    }

    [SetUp]
    public async Task OpenNamespaceAsync()
    {
        _timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        // A digit prefix requires quoting and a mixed-case input verifies URI/SQL normalization.
        _configuredSchema = "1MixedCase_" + Guid.NewGuid().ToString("N");
        _connection = new NpgsqlConnection(_container!.GetConnectionString());
        await _connection.OpenAsync(Token);
    }

    [TearDown]
    public async Task DropNamespaceAsync()
    {
        try
        {
            if (_connection is { State: System.Data.ConnectionState.Open })
            {
                await using var cleanup = new NpgsqlCommand($"DROP SCHEMA IF EXISTS \"{Schema}\" CASCADE", _connection);
                await cleanup.ExecuteNonQueryAsync(CancellationToken.None);
            }
        }
        finally
        {
            // Release the connection and the timeout even when dropping the schema failed.
            if (_connection is not null)
            {
                await _connection.DisposeAsync();
                _connection = null;
            }

            _timeout.Dispose();
        }
    }

    [Test]
    public async Task PutAsync_WithMixedCaseSchema_ReturnsLocatorInLowercaseNamespace()
    {
        // Arrange
        var repository = await CreateRepositoryAsync();

        // Act
        var uri = await PutAsync(repository, [1, 2, 3]);

        // Assert
        uri.Host.ShouldBe(Schema);
    }

    [TestCase(false, 0)]
    [TestCase(false, MultiChunkPayloadLength)]
    [TestCase(true, 0)]
    [TestCase(true, MultiChunkPayloadLength)]
    public async Task GetAsync_ReturnsStoredPayload(bool gzip, int payloadLength)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        var payload = RandomNumberGenerator.GetBytes(payloadLength);
        var uri = await PutAsync(repository, payload);

        // Act
        var stored = await ReadAsync(repository, uri);

        // Assert
        stored.ShouldBe(payload);
    }

    [TestCase(false, 0)]
    [TestCase(false, MultiChunkPayloadLength)]
    [TestCase(true, 0)]
    [TestCase(true, MultiChunkPayloadLength)]
    public async Task PutAsync_RecordsDecodedLengthAndSha256(bool gzip, int payloadLength)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        var payload = RandomNumberGenerator.GetBytes(payloadLength);

        // Act
        var uri = await PutAsync(repository, payload);

        // Assert
        var file = await ReadFileRowAsync(repository.ParseAddress(uri));
        file.Length.ShouldBe(payload.LongLength);
        file.Sha256.ShouldBe(Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant());
    }

    [TestCase(false, null)]
    [TestCase(true, "gzip")]
    public async Task PutAsync_RecordsEncoding(bool gzip, string? expectedEncoding)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);

        // Act
        var uri = await PutAsync(repository, [1, 2, 3]);

        // Assert
        var file = await ReadFileRowAsync(repository.ParseAddress(uri));
        file.Encoding.ShouldBe(expectedEncoding);
    }

    [Test]
    public async Task GetAsync_WhenPayloadExpired_ThrowsFileNotFound()
    {
        // Arrange
        var repository = await CreateRepositoryAsync();
        var expired = await PutAsync(repository, [1, 2, 3], TimeSpan.FromMinutes(-1));

        // Act
        var get = () => repository.GetAsync(expired, Token);

        // Assert
        await Should.ThrowAsync<FileNotFoundException>(get);
        var available = await PutAsync(repository, [4, 5, 6]);
        (await ReadAsync(repository, available)).ShouldBe(new byte[] { 4, 5, 6 });
    }

    [Test]
    public async Task DeleteExpiredAsync_DeletesExpiredPayloadAndItsChunks()
    {
        // Arrange
        var repository = await CreateRepositoryAsync();
        var expired = await PutAsync(repository, [1, 2, 3], TimeSpan.FromMinutes(-1));

        // Act
        var deleted = await repository.DeleteExpiredAsync(Token);

        // Assert
        deleted.ShouldBe(1);
        (await CountChunksAsync(repository.ParseAddress(expired))).ShouldBe(0);
        await Should.ThrowAsync<FileNotFoundException>(() => repository.GetAsync(expired, Token));
    }

    [Test]
    public async Task DeleteExpiredAsync_KeepsPayloadsThatHaveNotExpired()
    {
        // Arrange
        var repository = await CreateRepositoryAsync();
        byte[] payload = [1, 2, 3];
        var withoutExpiry = await PutAsync(repository, payload);
        var notYetExpired = await PutAsync(repository, payload, TimeSpan.FromHours(1));

        // Act
        var deleted = await repository.DeleteExpiredAsync(Token);

        // Assert
        deleted.ShouldBe(0);
        (await ReadAsync(repository, withoutExpiry)).ShouldBe(payload);
        (await ReadAsync(repository, notYetExpired)).ShouldBe(payload);
    }

    [TestCase(ChunkSizeBytes - 1, new[] { ChunkSizeBytes - 1 })]
    [TestCase(ChunkSizeBytes, new[] { ChunkSizeBytes })]
    [TestCase(ChunkSizeBytes + 1, new[] { ChunkSizeBytes, 1 })]
    [TestCase(2 * ChunkSizeBytes, new[] { ChunkSizeBytes, ChunkSizeBytes })]
    public async Task PutAsync_SplitsRawBytesAtExactChunkBoundaries(int length, int[] expectedSizes)
    {
        // Arrange
        var repository = await CreateRepositoryAsync();
        var payload = RandomNumberGenerator.GetBytes(length);

        // Act
        var uri = await PutAsync(repository, payload);

        // Assert
        var chunks = await ReadChunksAsync(repository.ParseAddress(uri));
        chunks.Select(chunk => chunk.Length).ShouldBe(expectedSizes);
        chunks.SelectMany(chunk => chunk).ShouldBe(payload);
    }

    [TestCase(0, ChunkSizeBytes)]
    [TestCase(int.MaxValue, 4 * 1024 * 1024)]
    public async Task PutAsync_ClampsConfiguredChunkSize(int configuredSize, int effectiveSize)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(chunkSizeBytes: configuredSize);
        var payload = RandomNumberGenerator.GetBytes(effectiveSize + 1);

        // Act
        var uri = await PutAsync(repository, payload);

        // Assert
        var id = repository.ParseAddress(uri);
        var chunks = await ReadChunksAsync(id);
        chunks.Select(chunk => chunk.Length).ShouldBe(new[] { effectiveSize, 1 });
        await using var command = new NpgsqlCommand($"SELECT chunk_size FROM \"{Schema}\".files WHERE id=@id", _connection);
        command.Parameters.AddWithValue("id", id);
        (await command.ExecuteScalarAsync(Token)).ShouldBe(effectiveSize);
    }

    [TestCase(0)]
    [TestCase(MultiChunkPayloadLength)]
    public async Task PutAsync_GzipChunksDecodeWithoutRepositoryReader(int length)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip: true);
        var payload = RandomNumberGenerator.GetBytes(length);

        // Act
        var uri = await PutAsync(repository, payload);

        // Assert
        var chunks = await ReadChunksAsync(repository.ParseAddress(uri));
        chunks.ShouldNotBeEmpty();
        chunks.Take(chunks.Count - 1).Select(chunk => chunk.Length).ShouldAllBe(size => size == ChunkSizeBytes);
        chunks[^1].Length.ShouldBeInRange(1, ChunkSizeBytes);
        using var encoded = new MemoryStream(chunks.SelectMany(chunk => chunk).ToArray());
        await using var gzip = new GZipStream(encoded, CompressionMode.Decompress);
        using var decoded = new MemoryStream();
        await gzip.CopyToAsync(decoded, Token);
        decoded.ToArray().ShouldBe(payload);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task GetAsync_ReadsExternallyWrittenChunksUsingStoredEncoding(bool gzip)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip: !gzip);
        var payload = RandomNumberGenerator.GetBytes(MultiChunkPayloadLength);
        using var encoded = new MemoryStream();
        if (gzip)
        {
            await using var compressor = new GZipStream(encoded, CompressionMode.Compress, leaveOpen: true);
            await compressor.WriteAsync(payload, Token);
        }
        else
        {
            await encoded.WriteAsync(payload, Token);
        }
        var id = Guid.NewGuid();
        await using (var header = new NpgsqlCommand($"""
            INSERT INTO "{Schema}".files(id, encoding, length, chunk_size, sha256)
            VALUES (@id, @encoding, @length, @chunkSize, @sha256)
            """, _connection))
        {
            header.Parameters.AddWithValue("id", id);
            header.Parameters.AddWithValue("encoding", gzip ? "GZIP" : DBNull.Value);
            header.Parameters.AddWithValue("length", payload.LongLength);
            header.Parameters.AddWithValue("chunkSize", ChunkSizeBytes);
            header.Parameters.AddWithValue("sha256", Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant());
            await header.ExecuteNonQueryAsync(Token);
        }
        var bytes = encoded.ToArray();
        // Insert out of order to establish that n, rather than insertion order, controls reads.
        for (var n = (bytes.Length - 1) / ChunkSizeBytes; n >= 0; n--)
        {
            await using var chunk = new NpgsqlCommand($"INSERT INTO \"{Schema}\".chunks(file_id, n, data) VALUES (@id, @n, @data)", _connection);
            chunk.Parameters.AddWithValue("id", id);
            chunk.Parameters.AddWithValue("n", n);
            chunk.Parameters.AddWithValue("data", bytes.Skip(n * ChunkSizeBytes).Take(ChunkSizeBytes).ToArray());
            await chunk.ExecuteNonQueryAsync(Token);
        }

        // Act
        var stored = await ReadAsync(repository, new Uri($"pgbin://{Schema}/files/{id:D}"));

        // Assert
        stored.ShouldBe(payload);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task PutAsync_WhenSourceFails_RollsBackAndLeavesInputOpen(bool gzip)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        using var input = new FailingInputStream(RandomNumberGenerator.GetBytes(MultiChunkPayloadLength));

        // Act
        var exception = await Should.ThrowAsync<IOException>(() => repository.PutAsync(input, cancellationToken: Token));

        // Assert
        exception.ShouldBeSameAs(input.Failure);
        input.CanRead.ShouldBeTrue();
        await AssertEmptyStorageAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task PutAsync_WhenSourceCancels_RollsBackAndLeavesInputOpen(bool gzip)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
        using var input = new FailingInputStream(RandomNumberGenerator.GetBytes(MultiChunkPayloadLength), cancellation);

        // Act
        await Should.ThrowAsync<OperationCanceledException>(() => repository.PutAsync(input, cancellationToken: cancellation.Token));

        // Assert
        input.CanRead.ShouldBeTrue();
        await AssertEmptyStorageAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task PutAsync_LeavesInputOpenOnSuccess(bool gzip)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        using var input = new MemoryStream(new byte[] { 1, 2, 3 });

        // Act
        var uri = await repository.PutAsync(input, cancellationToken: Token);

        // Assert
        input.CanRead.ShouldBeTrue();
        input.Position.ShouldBe(3);
        (await ReadAsync(repository, uri)).ShouldBe(new byte[] { 1, 2, 3 });
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task GetAsync_SynchronousDisposalReleasesConnection(bool gzip, bool partialRead)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        var uri = await PutAsync(repository, RandomNumberGenerator.GetBytes(MultiChunkPayloadLength));
        using var stream = await repository.GetAsync(uri, Token);
        if (partialRead)
        {
            stream.ReadByte().ShouldBeGreaterThanOrEqualTo(0);
        }

        // Act
        stream.Dispose();
        stream.Dispose();

        // Assert
        stream.CanRead.ShouldBeFalse();
        // This pool has only one connection; a leak makes the next operation time out.
        (await ReadAsync(repository, uri)).Length.ShouldBe(MultiChunkPayloadLength);
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task GetAsync_AsynchronousDisposalReleasesConnection(bool gzip, bool partialRead)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        var uri = await PutAsync(repository, RandomNumberGenerator.GetBytes(MultiChunkPayloadLength));
        await using var stream = await repository.GetAsync(uri, Token);
        if (partialRead)
        {
            (await stream.ReadAsync(new byte[1], Token)).ShouldBe(1);
        }

        // Act
        await stream.DisposeAsync();
        await stream.DisposeAsync();

        // Assert
        stream.CanRead.ShouldBeFalse();
        (await ReadAsync(repository, uri)).Length.ShouldBe(MultiChunkPayloadLength);
    }

    [Test]
    public async Task GetAsync_MissingPayloadReleasesConnection()
    {
        // Arrange
        var repository = await CreateRepositoryAsync();
        var missing = new Uri($"pgbin://{Schema}/files/{Guid.NewGuid():D}");

        // Act
        await Should.ThrowAsync<FileNotFoundException>(() => repository.GetAsync(missing, Token));

        // Assert
        var uri = await PutAsync(repository, [1, 2, 3]);
        (await ReadAsync(repository, uri)).ShouldBe(new byte[] { 1, 2, 3 });
    }

    [Test]
    public async Task GetAsync_CancelledReadRejectsBufferedDataAndReleasesConnection()
    {
        // Arrange
        var repository = await CreateRepositoryAsync();
        var uri = await PutAsync(repository, [1, 2, 3]);
        await using var stream = await repository.GetAsync(uri, Token);
        (await stream.ReadAsync(new byte[1], Token)).ShouldBe(1);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        // Act
        await Should.ThrowAsync<OperationCanceledException>(() => stream.ReadAsync(new byte[1], cancellation.Token).AsTask());
        await stream.DisposeAsync();

        // Assert
        (await ReadAsync(repository, uri)).ShouldBe(new byte[] { 1, 2, 3 });
    }

    [Test]
    public async Task GetAsync_EmptyReadDoesNotAdvancePayload()
    {
        // Arrange
        var repository = await CreateRepositoryAsync();
        var uri = await PutAsync(repository, [1, 2, 3]);
        await using var stream = await repository.GetAsync(uri, Token);

        // Act
        var read = await stream.ReadAsync(Memory<byte>.Empty, Token);

        // Assert
        read.ShouldBe(0);
        stream.ReadByte().ShouldBe(1);
    }

    [Test]
    public async Task PutAsync_EmptyRawPayloadHasNoChunks()
    {
        // Arrange
        var repository = await CreateRepositoryAsync();

        // Act
        var uri = await PutAsync(repository, []);

        // Assert
        (await CountChunksAsync(repository.ParseAddress(uri))).ShouldBe(0);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Dispatch_MaterializedPayloadAtDecodedLimitReleasesConnection(bool gzip)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        var payload = new byte[4096];
        var uri = await PutAsync(repository, payload);
        var capture = new PayloadCapture();
        await using var provider = ConfigureDispatch<BytesMessage, BytesConsumer>(repository, capture);

        // Act
        await DispatchAsync(provider, uri);

        // Assert
        capture.Bytes.ShouldBe(payload);
        (await ReadAsync(repository, uri)).ShouldBe(payload);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Dispatch_MaterializedPayloadAboveDecodedLimitFailsAndReleasesConnection(bool gzip)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        var uri = await PutAsync(repository, new byte[4097]);
        var capture = new PayloadCapture();
        await using var provider = ConfigureDispatch<BytesMessage, BytesConsumer>(repository, capture);

        // Act
        var exception = await Should.ThrowAsync<InvalidDataException>(() => DispatchAsync(provider, uri));

        // Assert
        exception.Message.ShouldContain("4096 byte limit");
        capture.Bytes.ShouldBeNull();
        (await ReadAsync(repository, uri)).Length.ShouldBe(4097);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Dispatch_ConsumerOwnedStreamAtDecodedLimitReleasesConnection(bool gzip)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        var payload = new byte[4096];
        var uri = await PutAsync(repository, payload);
        var capture = new PayloadCapture();
        await using var provider = ConfigureDispatch<StreamMessage, StreamConsumer>(repository, capture);

        // Act
        await DispatchAsync(provider, uri);

        // Assert
        capture.Bytes.ShouldBe(payload);
        (await ReadAsync(repository, uri)).ShouldBe(payload);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Dispatch_ConsumerReadAboveDecodedLimitFailsAndReleasesConnection(bool gzip)
    {
        // Arrange
        var repository = await CreateRepositoryAsync(gzip);
        var uri = await PutAsync(repository, new byte[4097]);
        var capture = new PayloadCapture();
        await using var provider = ConfigureDispatch<StreamMessage, StreamConsumer>(repository, capture);

        // Act
        var exception = await Should.ThrowAsync<InvalidDataException>(() => DispatchAsync(provider, uri));

        // Assert
        exception.Message.ShouldContain("4096 byte limit");
        capture.Bytes.ShouldBeNull();
        (await ReadAsync(repository, uri)).Length.ShouldBe(4097);
    }

    private static ServiceProvider ConfigureDispatch<TMessage, TConsumer>(IMessageDataRepository repository, PayloadCapture capture)
        where TMessage : class
        where TConsumer : class, IConsumer<TMessage>
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(repository);
        services.AddSingleton(capture);
        services.AddSingleton<IEndpointProvider, UnexpectedEndpoints>();
        services.AddConveyo(builder =>
        {
            builder.Map<TMessage>("test:stored-payload.v1");
            builder.AddConsumer<TConsumer>();
            builder.MaxMessageDataBytes(4096);
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private Task DispatchAsync(ServiceProvider provider, Uri address) =>
        provider.GetRequiredService<MessageDispatcher>().DispatchAsync(new MessageEnvelope
        {
            MessageType = ["test:stored-payload.v1"],
            Message = JsonSerializer.SerializeToElement(new { payload = new { address = address.ToString() } })
        }, Token);

    private async Task<PostgresMessageDataRepository> CreateRepositoryAsync(bool gzip = false, int chunkSizeBytes = ChunkSizeBytes)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(_container!.GetConnectionString())
        {
            ApplicationName = Schema,
            MaxPoolSize = 1,
            Timeout = 2
        }.ConnectionString;
        var repository = new PostgresMessageDataRepository(
            connectionString, _configuredSchema, chunkSizeBytes, gzip);
        await repository.EnsureSchemaAsync(Token);
        return repository;
    }

    private async Task<Uri> PutAsync(PostgresMessageDataRepository repository, byte[] payload, TimeSpan? timeToLive = null)
    {
        using var input = new MemoryStream(payload);
        return await repository.PutAsync(input, timeToLive, Token);
    }

    private async Task<byte[]> ReadAsync(PostgresMessageDataRepository repository, Uri uri)
    {
        await using var stream = await repository.GetAsync(uri, Token);
        using var output = new MemoryStream();
        await stream.CopyToAsync(output, Token);
        return output.ToArray();
    }

    private async Task<StoredFile> ReadFileRowAsync(Guid id)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT length, sha256, encoding FROM \"{Schema}\".\"files\" WHERE id=@id", _connection);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(Token);
        (await reader.ReadAsync(Token)).ShouldBeTrue("the files table has no row for the stored payload");
        return new StoredFile(reader.GetInt64(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2));
    }

    private async Task<long> CountChunksAsync(Guid id)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT count(*) FROM \"{Schema}\".\"chunks\" WHERE file_id=@id", _connection);
        command.Parameters.AddWithValue("id", id);
        return (long)(await command.ExecuteScalarAsync(Token))!;
    }

    private sealed record StoredFile(long Length, string Sha256, string? Encoding);

    private async Task<List<byte[]>> ReadChunksAsync(Guid id)
    {
        await using var command = new NpgsqlCommand($"SELECT n, data FROM \"{Schema}\".chunks WHERE file_id=@id ORDER BY n", _connection);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(Token);
        var chunks = new List<byte[]>();
        while (await reader.ReadAsync(Token))
        {
            reader.GetInt32(0).ShouldBe(chunks.Count);
            chunks.Add((byte[])reader[1]);
        }
        return chunks;
    }

    private async Task AssertEmptyStorageAsync()
    {
        await using var command = new NpgsqlCommand($"SELECT (SELECT count(*) FROM \"{Schema}\".files) + (SELECT count(*) FROM \"{Schema}\".chunks)", _connection);
        (await command.ExecuteScalarAsync(Token)).ShouldBe(0L);
    }

    private sealed class FailingInputStream(byte[] bytes, CancellationTokenSource? cancellation = null) : MemoryStream(bytes)
    {
        public IOException Failure { get; } = new("source read failed");

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (Position == 0)
            {
                return base.ReadAsync(buffer, offset, count, cancellationToken);
            }
            cancellation?.Cancel();
            cancellationToken.ThrowIfCancellationRequested();
            throw Failure;
        }
    }

    private sealed record BytesMessage(MessageData<byte[]> Payload);
    private sealed record StreamMessage(MessageData<Stream> Payload);
    private sealed class PayloadCapture
    {
        public byte[]? Bytes { get; set; }
    }

    private sealed class BytesConsumer(PayloadCapture capture) : IConsumer<BytesMessage>
    {
        public Task Consume(ConsumeContext<BytesMessage> context)
        {
            capture.Bytes = context.Message.Payload.Value;
            return Task.CompletedTask;
        }
    }

    private sealed class StreamConsumer(PayloadCapture capture) : IConsumer<StreamMessage>
    {
        public async Task Consume(ConsumeContext<StreamMessage> context)
        {
            await using var stream = context.Message.Payload.Value!;
            using var output = new MemoryStream();
            await stream.CopyToAsync(output, context.CancellationToken);
            capture.Bytes = output.ToArray();
        }
    }

    private sealed class UnexpectedEndpoints : IEndpointProvider
    {
        public IPublishEndpoint GetPublishEndpoint<T>() where T : class => throw new NotSupportedException();
        public ISendEndpoint GetSendEndpoint<T>() where T : class => throw new NotSupportedException();
        public ISendEndpoint GetSendEndpoint<T>(Uri address) where T : class => throw new NotSupportedException();
    }
}
