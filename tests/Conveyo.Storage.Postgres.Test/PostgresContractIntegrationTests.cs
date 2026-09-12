using System.Security.Cryptography;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Conveyo.Storage.Postgres.Test;

[TestFixture]
[Category("Integration")]
internal class PostgresContractIntegrationTests
{
    private PostgreSqlContainer? _container;

    [OneTimeSetUp]
    public async Task StartAsync()
    {
        _container = new PostgreSqlBuilder("postgres:18.6-alpine").Build();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        try
        {
            await _container.StartAsync(timeout.Token);
        }
        catch
        {
            await StopAsync();
            throw;
        }
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

    [TestCase(false)]
    [TestCase(true)]
    public async Task QuotedNamespaceRoundTripAndExpiry(bool gzip)
    {
        var connectionString = _container!.GetConnectionString();
        // A digit prefix requires quoting and a mixed-case input verifies URI/SQL normalization.
        var configuredSchema = "1Stage1_" + Guid.NewGuid().ToString("N");
        var schema = configuredSchema.ToLowerInvariant();
        var repository = new PostgresMessageDataRepository(connectionString, configuredSchema, 65536, gzip);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var token = timeout.Token;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(token);
        try
        {
            await repository.EnsureSchemaAsync(token);
            foreach (var payload in new[] { Array.Empty<byte>(), RandomNumberGenerator.GetBytes(150000) })
            {
                using var input = new MemoryStream(payload);
                var uri = await repository.PutAsync(input, cancellationToken: token);
                Assert.That(uri.Host, Is.EqualTo(schema));
                await using (var stream = await repository.GetAsync(uri, token))
                {
                    using var output = new MemoryStream();
                    await stream.CopyToAsync(output, token);
                    Assert.That(output.ToArray(), Is.EqualTo(payload));
                }
                await using var metadata = new NpgsqlCommand($"SELECT length, sha256, encoding FROM \"{schema}\".\"files\" WHERE id=@id", connection);
                metadata.Parameters.AddWithValue("id", repository.ParseAddress(uri));
                await using (var reader = await metadata.ExecuteReaderAsync(token))
                {
                    Assert.That(await reader.ReadAsync(token), Is.True);
                    Assert.That(reader.GetInt64(0), Is.EqualTo(payload.LongLength));
                    Assert.That(reader.GetString(1), Is.EqualTo(Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant()));
                    Assert.That(reader.IsDBNull(2) ? null : reader.GetString(2), Is.EqualTo(gzip ? "gzip" : null));
                }
            }
            using var expiredInput = new MemoryStream(new byte[] { 1, 2, 3 });
            var expired = await repository.PutAsync(expiredInput, TimeSpan.FromMinutes(-1), token);
            Assert.ThrowsAsync<FileNotFoundException>(() => repository.GetAsync(expired, token));
            Assert.That(await repository.DeleteExpiredAsync(token), Is.EqualTo(1));
            Assert.ThrowsAsync<FileNotFoundException>(() => repository.GetAsync(expired, token));
            await using var chunks = new NpgsqlCommand($"SELECT count(*) FROM \"{schema}\".\"chunks\" WHERE file_id=@id", connection);
            chunks.Parameters.AddWithValue("id", repository.ParseAddress(expired));
            Assert.That(await chunks.ExecuteScalarAsync(token), Is.EqualTo(0L));
        }
        finally
        {
            await using var cleanup = new NpgsqlCommand($"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE", connection);
            await cleanup.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }
}
