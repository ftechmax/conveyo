using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Conveyo.Storage.Postgres.Test;

[TestFixture]
public class StorageConfiguratorExtensionsTests
{
    [Test]
    public void AddPostgresMessageData_RegistersRepositoryAndSchemaInitializer()
    {
        var services = new ServiceCollection();

        services.AddPostgresMessageData(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin");

        Assert.That(services.Any(d => d.ServiceType == typeof(PostgresMessageDataRepository)), Is.True);
        Assert.That(services.Any(d => d.ServiceType == typeof(IMessageDataRepository)), Is.True);
        Assert.That(
            services.Any(d => d.ServiceType == typeof(IHostedService)
                              && d.ImplementationType?.Name == "PostgresMessageDataSchemaInitializerHostedService"),
            Is.True);

        using var provider = services.BuildServiceProvider();
        var repository = provider.GetRequiredService<PostgresMessageDataRepository>();
        var resolved = provider.GetRequiredService<IMessageDataRepository>();
        Assert.That(resolved, Is.SameAs(repository));
    }

    [Test]
    public void GetAsync_RejectsLocatorForDifferentSchema()
    {
        var repository = new PostgresMessageDataRepository(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin",
            schema: "md");

        var ex = Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            repository.GetAsync(new Uri("pgbin://other/files/0194ad8f-61a2-7f28-9001-111111111111")));

        Assert.That(ex!.Message, Does.Contain("pgbin://md/files"));
    }

    [Test]
    public void GetAsync_RejectsLocatorForDifferentPathSegment()
    {
        var repository = new PostgresMessageDataRepository(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin",
            schema: "md");

        var ex = Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            repository.GetAsync(new Uri("pgbin://md/chunks/0194ad8f-61a2-7f28-9001-111111111111")));

        Assert.That(ex!.Message, Does.Contain("pgbin://md/files"));
    }

    [Test]
    public void GetAsync_RejectsLocatorWithNonCanonicalComponents()
    {
        var repository = new PostgresMessageDataRepository(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin",
            schema: "md");

        Assert.ThrowsAsync<FormatException>(() =>
            repository.GetAsync(new Uri("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111?read=1")));
    }

    [Test]
    public void GetAsync_AcceptsLocatorWhenSchemaConfiguredWithMixedCase()
    {
        // A mixed-case schema must still accept the (lowercase, Uri-canonicalized) locator it generates.
        // The short timeout lets the subsequent expected DB failure surface quickly.
        var repository = new PostgresMessageDataRepository(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin;Timeout=1;Command Timeout=1",
            schema: "MySchema");

        var ex = Assert.CatchAsync(() =>
            repository.GetAsync(new Uri("pgbin://myschema/files/0194ad8f-61a2-7f28-9001-111111111111")));

        Assert.That(ex, Is.Not.InstanceOf<UnauthorizedAccessException>());
    }
}
