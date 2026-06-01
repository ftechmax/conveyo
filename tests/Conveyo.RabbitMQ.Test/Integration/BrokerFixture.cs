using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace Conveyo.RabbitMQ.Test.Integration;

/// <summary>
/// Resolves RabbitMQ broker connection details from environment variables and skips integration
/// tests when no broker is configured. This lets CI run the same tests against a real broker
/// container while keeping developer-local <c>dotnet test</c> runs hermetic.
/// </summary>
internal static class BrokerFixture
{
    public const string HostEnvVar = "CONVEYO_RABBITMQ_HOST";
    public const string PortEnvVar = "CONVEYO_RABBITMQ_PORT";
    public const string UserEnvVar = "CONVEYO_RABBITMQ_USER";
    public const string PassEnvVar = "CONVEYO_RABBITMQ_PASS";
    public const string VHostEnvVar = "CONVEYO_RABBITMQ_VHOST";

    public static RabbitMqHostOptions? TryGetOptions(string clientNameSuffix)
    {
        var host = Environment.GetEnvironmentVariable(HostEnvVar);
        if (string.IsNullOrWhiteSpace(host))
        {
            return null;
        }

        var port = int.TryParse(Environment.GetEnvironmentVariable(PortEnvVar), out var p) ? p : 5672;
        var user = Environment.GetEnvironmentVariable(UserEnvVar) ?? "guest";
        var pass = Environment.GetEnvironmentVariable(PassEnvVar) ?? "guest";
        var vhost = Environment.GetEnvironmentVariable(VHostEnvVar) ?? "/";

        return new RabbitMqHostOptions
        {
            ClientName = $"Conveyo.IntegrationTests/{clientNameSuffix}",
            Host = host,
            Port = port,
            VHost = vhost,
            Username = user,
            Password = pass
        };
    }

    public static void SkipIfBrokerMissing()
    {
        if (TryGetOptions("probe") is null)
        {
            Assert.Ignore($"Skipping RabbitMQ integration test: set {HostEnvVar} (and optionally {PortEnvVar}/{UserEnvVar}/{PassEnvVar}/{VHostEnvVar}) to enable.");
        }
    }

    public static async Task<RabbitMqConnectionManager> StartConnectionAsync(string clientNameSuffix, ushort prefetchCount = 16, CancellationToken cancellationToken = default)
    {
        var options = TryGetOptions(clientNameSuffix)
            ?? throw new InvalidOperationException("Broker options unavailable; call SkipIfBrokerMissing first.");
        options.PrefetchCount = prefetchCount;
        var manager = new RabbitMqConnectionManager(NullLogger.Instance);
        await manager.StartAsync(options, cancellationToken);
        return manager;
    }

    public static async Task<string> DeclareTransientQueueAsync(IChannel channel, string suffix, CancellationToken cancellationToken = default)
    {
        var queue = $"conveyo-it-{suffix}-{Guid.NewGuid():N}";
        await channel.QueueDeclareAsync(
            queue: queue,
            durable: false,
            exclusive: false,
            autoDelete: true,
            arguments: null,
            cancellationToken: cancellationToken);
        return queue;
    }
}
