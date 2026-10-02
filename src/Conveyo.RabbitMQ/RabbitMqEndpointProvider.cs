using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqEndpointProvider(
    RabbitMqBusRegistrationContext rabbitMqContext,
    ConveyoContext conveyoContext) : IEndpointProvider
{
    private readonly HostInfo _hostInfo = conveyoContext.HostInfo;

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

    private static Task EnsureExchangeDeclaredAsync(IChannel channel, string exchange, CancellationToken cancellationToken)
        => RabbitMqTopology.DeclareDurableFanoutExchangeAsync(channel, exchange, cancellationToken);

    private static bool IsFaultType(Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Fault<>);

}
