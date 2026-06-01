using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Conveyo.Storage.Postgres;

public static class StorageConfiguratorExtensions
{
    /// <summary>Default interval between expired-payload cleanup sweeps.</summary>
    public static readonly TimeSpan DefaultCleanupInterval = TimeSpan.FromMinutes(5);

    public static IServiceCollection AddPostgresMessageData(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var connectionString = configuration["postgres:connection-string"]
            ?? throw new InvalidOperationException(ErrorMessages.MissingConnectionStringConfiguration);
        var schema = configuration["conveyo:storage:schema"] ?? PostgresMessageDataRepository.DefaultSchema;
        var chunkSizeBytes = int.TryParse(configuration["conveyo:storage:chunkSizeBytes"], out var chunkSize)
            ? chunkSize
            : PostgresMessageDataRepository.DefaultChunkSizeBytes;
        var gzip = bool.TryParse(configuration["conveyo:storage:gzip"], out var parsedGzip) && parsedGzip;
        var cleanupInterval = TimeSpan.TryParse(
            configuration["conveyo:storage:cleanupInterval"], CultureInfo.InvariantCulture, out var parsedInterval)
            ? parsedInterval
            : DefaultCleanupInterval;

        return services.AddPostgresMessageData(connectionString, schema, chunkSizeBytes, gzip, cleanupInterval);
    }

    public static IServiceCollection AddPostgresMessageData(
        this IServiceCollection services,
        string connectionString,
        string schema = PostgresMessageDataRepository.DefaultSchema,
        int chunkSizeBytes = PostgresMessageDataRepository.DefaultChunkSizeBytes,
        bool gzip = false,
        TimeSpan? cleanupInterval = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton(_ => new PostgresMessageDataRepository(connectionString, schema, chunkSizeBytes, gzip));
        services.AddSingleton<IMessageDataRepository>(sp => sp.GetRequiredService<PostgresMessageDataRepository>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, PostgresMessageDataSchemaInitializerHostedService>());

        var effectiveInterval = cleanupInterval ?? DefaultCleanupInterval;
        if (effectiveInterval > TimeSpan.Zero)
        {
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, PostgresMessageDataCleanupHostedService>(
                sp => new PostgresMessageDataCleanupHostedService(
                    sp.GetRequiredService<PostgresMessageDataRepository>(),
                    sp.GetRequiredService<ILogger<PostgresMessageDataCleanupHostedService>>(),
                    effectiveInterval)));
        }

        return services;
    }

    public static IConveyoBuilder AddPostgresMessageData(this IConveyoBuilder builder, IConfiguration configuration)
    {
        builder.Services.AddPostgresMessageData(configuration);
        return builder;
    }

    public static IConveyoBuilder AddPostgresMessageData(
        this IConveyoBuilder builder,
        string connectionString,
        string schema = PostgresMessageDataRepository.DefaultSchema,
        int chunkSizeBytes = PostgresMessageDataRepository.DefaultChunkSizeBytes,
        bool gzip = false,
        TimeSpan? cleanupInterval = null)
    {
        builder.Services.AddPostgresMessageData(connectionString, schema, chunkSizeBytes, gzip, cleanupInterval);
        return builder;
    }
}

internal sealed class PostgresMessageDataCleanupHostedService(
    PostgresMessageDataRepository repository,
    ILogger<PostgresMessageDataCleanupHostedService> logger,
    TimeSpan interval) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(interval);
        do
        {
            try
            {
                var deleted = await repository.DeleteExpiredAsync(stoppingToken);
                if (deleted > 0)
                {
                    logger.LogInformation(LogMessages.ExpiredMessageDataDeleted, deleted);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, LogMessages.ExpiredMessageDataCleanupFailed);
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}

internal sealed class PostgresMessageDataSchemaInitializerHostedService(
    PostgresMessageDataRepository repository,
    ILogger<PostgresMessageDataSchemaInitializerHostedService> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(LogMessages.EnsuringMessageDataSchema);
        await repository.EnsureSchemaAsync(cancellationToken);
        logger.LogInformation(LogMessages.MessageDataSchemaReady);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
