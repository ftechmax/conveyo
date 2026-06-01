using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

internal static class RabbitMqTopology
{
    public static Task DeclareDurableQueueAsync(IChannel channel, string queueName, CancellationToken cancellationToken)
        => channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

    public static Task DeclareDurableFanoutExchangeAsync(IChannel channel, string exchange, CancellationToken cancellationToken)
        => channel.ExchangeDeclareAsync(
            exchange: exchange,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);
}
