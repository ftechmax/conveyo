using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Conveyo.Storage.Postgres.Test;

[TestFixture]
public class StorageConfiguratorExtensionsTests
{
    [Test]
    public void AddPostgresMessageData_RegistersRepositoryAndSchemaInitializer()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddPostgresMessageData(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin");

        // Assert
        services.Any(d => d.ServiceType == typeof(PostgresMessageDataRepository)).ShouldBeTrue();
        services.Any(d => d.ServiceType == typeof(IMessageDataRepository)).ShouldBeTrue();
        services.Any(d => d.ServiceType == typeof(IHostedService)
                              && d.ImplementationType?.Name == "PostgresMessageDataSchemaInitializerHostedService").ShouldBeTrue();
    }

    [Test]
    public void AddPostgresMessageData_ResolvesRepositoryInterfaceToSameSingleton()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddPostgresMessageData("Host=localhost;Database=conveyo;Username=admin;Password=admin");
        using var provider = services.BuildServiceProvider();

        // Act
        var repository = provider.GetRequiredService<PostgresMessageDataRepository>();
        var resolved = provider.GetRequiredService<IMessageDataRepository>();

        // Assert
        resolved.ShouldBeSameAs(repository);
    }

    [Test]
    public async Task GetAsync_RejectsLocatorForDifferentSchema()
    {
        // Arrange
        var repository = new PostgresMessageDataRepository(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin",
            schema: "md");

        // Act
        var ex = await Should.ThrowAsync<UnauthorizedAccessException>(() =>
            repository.GetAsync(new Uri("pgbin://other/files/0194ad8f-61a2-7f28-9001-111111111111")));

        // Assert
        ex.Message.ShouldContain("pgbin://md/files");
    }

    [Test]
    public async Task GetAsync_RejectsLocatorForDifferentPathSegment()
    {
        // Arrange
        var repository = new PostgresMessageDataRepository(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin",
            schema: "md");

        // Act
        var ex = await Should.ThrowAsync<UnauthorizedAccessException>(() =>
            repository.GetAsync(new Uri("pgbin://md/chunks/0194ad8f-61a2-7f28-9001-111111111111")));

        // Assert
        ex.Message.ShouldContain("pgbin://md/files");
    }

    [Test]
    public async Task GetAsync_RejectsLocatorWithNonCanonicalComponents()
    {
        // Arrange
        var repository = new PostgresMessageDataRepository(
            connectionString: "Host=localhost;Database=conveyo;Username=admin;Password=admin",
            schema: "md");

        // Act
        var operation = () =>
            repository.GetAsync(new Uri("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111?read=1"));

        // Assert
        await Should.ThrowAsync<FormatException>(operation);
    }
}
