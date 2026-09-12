using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace Conveyo.RabbitMQ.Test.Integration;

/// <summary>
/// Shares one broker across this namespace. Create it during setup so test discovery
/// does not require a container runtime.
/// </summary>
[SetUpFixture]
public sealed class BrokerFixture
{
    private static RabbitMqContainer? _container;

    [OneTimeSetUp]
    public async Task StartAsync()
    {
        _container = new RabbitMqBuilder("rabbitmq:4.3.0-management").Build();
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

    internal static RabbitMqHostOptions GetOptions(string clientNameSuffix)
    {
        var container = _container ?? throw new InvalidOperationException("RabbitMQ fixture has not started.");
        var connection = new ConnectionFactory { Uri = new Uri(container.GetConnectionString()) };
        return new RabbitMqHostOptions
        {
            ClientName = $"Conveyo.IntegrationTests/{clientNameSuffix}",
            Host = connection.HostName,
            Port = connection.Port,
            VHost = connection.VirtualHost,
            Username = connection.UserName,
            Password = connection.Password
        };
    }

    internal static async Task<RabbitMqConnectionManager> StartConnectionAsync(string clientNameSuffix, ushort prefetchCount = 16, CancellationToken cancellationToken = default)
    {
        var options = GetOptions(clientNameSuffix);
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
            exclusive: true,
            autoDelete: true,
            arguments: null,
            cancellationToken: cancellationToken);
        return queue;
    }
}
