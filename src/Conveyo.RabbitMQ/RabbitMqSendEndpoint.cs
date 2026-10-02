using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqSendEndpoint(
    Func<CancellationToken, Task<IChannel>> channelFactory,
    string queueName,
    HostInfo hostInfo,
    string urn) : ISendEndpoint
{
    // Commands require a route through the default exchange to the target queue.
    private readonly RabbitMqPublisher _publisher = new(
        channelFactory, destination: queueName, exchange: string.Empty, routingKey: queueName,
        mandatory: true, operation: "send", hostInfo, urn);

    public Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class
        => _publisher.PublishAsync(message, cancellationToken);
}
