using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqPublishEndpoint(
    Func<CancellationToken, Task<IChannel>> channelFactory,
    string exchangeName,
    HostInfo hostInfo,
    string urn,
    Func<IChannel, string, CancellationToken, Task>? ensureExchangeDeclaredAsync = null) : IPublishEndpoint
{
    // Events permit zero subscribers on the URN fanout exchange.
    private readonly RabbitMqPublisher _publisher = new(
        channelFactory, destination: exchangeName, exchange: exchangeName, routingKey: string.Empty,
        mandatory: false, operation: "publish", hostInfo, urn, ensureExchangeDeclaredAsync);

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class
        => _publisher.PublishAsync(message, cancellationToken);
}
