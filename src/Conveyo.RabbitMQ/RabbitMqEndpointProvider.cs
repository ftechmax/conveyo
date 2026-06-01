using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqEndpointProvider(
    RabbitMqBusRegistrationContext rabbitMqContext,
    ConveyoContext conveyoContext) : IEndpointProvider
{
    private readonly Dictionary<string, Task> _lazyDeclareTasks = new(StringComparer.Ordinal);
    private readonly HostInfo _hostInfo = conveyoContext.HostInfo;
    private readonly object _lazyDeclareLock = new();

    public IPublishEndpoint GetPublishEndpoint<T>() where T : class
    {
        var urn = conveyoContext.UrnFor(typeof(T));
        var ensureDeclared = IsFaultType(typeof(T)) ? (Func<IChannel, string, CancellationToken, Task>?)EnsureExchangeDeclaredAsync : null;
        return new RabbitMqPublishEndpoint(rabbitMqContext.CreatePublisherChannelAsync, urn, _hostInfo, urn, ensureDeclared);
    }

    public ISendEndpoint GetSendEndpoint<T>() where T : class
    {
        var urn = conveyoContext.UrnFor(typeof(T));
        var queueName = rabbitMqContext.GetQueueName(typeof(T));
        return new RabbitMqSendEndpoint(rabbitMqContext.CreatePublisherChannelAsync, queueName, _hostInfo, urn);
    }

    public ISendEndpoint GetSendEndpoint<T>(Uri address) where T : class
    {
        ArgumentNullException.ThrowIfNull(address);

        var urn = conveyoContext.UrnFor(typeof(T));
        var queueName = QueueAddress.GetQueueName(address);
        return new RabbitMqSendEndpoint(rabbitMqContext.CreatePublisherChannelAsync, queueName, _hostInfo, urn);
    }

    private async Task EnsureExchangeDeclaredAsync(IChannel channel, string exchange, CancellationToken cancellationToken)
    {
        Task task;
        lock (_lazyDeclareLock)
        {
            if (!_lazyDeclareTasks.TryGetValue(exchange, out task!))
            {
                task = RabbitMqTopology.DeclareDurableFanoutExchangeAsync(channel, exchange, cancellationToken);
                _lazyDeclareTasks[exchange] = task;
            }
        }

        try
        {
            await task;
        }
        catch
        {
            // Don't cache a failed declare, let the next caller retry.
            lock (_lazyDeclareLock)
            {
                if (_lazyDeclareTasks.TryGetValue(exchange, out var cached) && ReferenceEquals(cached, task))
                {
                    _lazyDeclareTasks.Remove(exchange);
                }
            }

            throw;
        }
    }

    private static bool IsFaultType(Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Fault<>);

}
