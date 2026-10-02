using System.Diagnostics;
using Conveyo.Diagnostics;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqPublisher(
    Func<CancellationToken, Task<IChannel>> channelFactory,
    string destination,
    string exchange,
    string routingKey,
    bool mandatory,
    string operation,
    HostInfo hostInfo,
    string urn,
    Func<IChannel, string, CancellationToken, Task>? ensureExchangeDeclaredAsync = null)
{
    public async Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        var envelope = EnvelopeSerializer.Create(message, hostInfo, urn);
        var body = EnvelopeSerializer.Serialize(envelope);

        using var activity = ConveyoActivitySource.StartProducer(
            RabbitMqDiagnosticHeaders.MessagingSystem,
            operation,
            destination,
            envelope);
        try
        {
            var properties = RabbitMqMessageProperties.ForEnvelope(envelope);
            RabbitMqTraceContextPropagation.Inject(activity, properties.Headers!);
            activity?.SetTag(RabbitMqDiagnosticHeaders.RoutingKey, routingKey);
            activity?.SetTag(DiagnosticHeaders.MessagingBodySize, body.Length);

            await using var channel = await channelFactory(cancellationToken);

            if (ensureExchangeDeclaredAsync is not null)
            {
                await ensureExchangeDeclaredAsync(channel, exchange, cancellationToken);
            }

            try
            {
                await channel.BasicPublishAsync(exchange, routingKey, mandatory, properties, body, cancellationToken);
            }
            catch (PublishReturnException returned) when (mandatory)
            {
                throw new UnroutableMessageException(
                    ErrorMessages.SendToQueueUnroutable(destination, returned.ReplyCode, returned.ReplyText), returned)
                {
                    Exchange = returned.Exchange,
                    RoutingKey = returned.RoutingKey,
                    ReplyCode = returned.ReplyCode,
                    ReplyText = returned.ReplyText
                };
            }
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }
}
