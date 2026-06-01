using System.Diagnostics;
using Conveyo.Diagnostics;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqSendEndpoint(
    Func<CancellationToken, Task<IChannel>> channelFactory,
    string queueName,
    HostInfo hostInfo,
    string urn) : ISendEndpoint
{
    public async Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        var envelope = EnvelopeSerializer.Create(message, hostInfo, urn);
        var body = EnvelopeSerializer.Serialize(envelope);

        using var activity = ConveyoActivitySource.StartProducer(
            RabbitMqDiagnosticHeaders.MessagingSystem,
            "send",
            queueName,
            envelope);
        try
        {
            var properties = RabbitMqMessageProperties.ForEnvelope(envelope);
            RabbitMqTraceContextPropagation.Inject(activity, properties.Headers!);
            activity?.SetTag(RabbitMqDiagnosticHeaders.RoutingKey, queueName);
            activity?.SetTag(DiagnosticHeaders.MessagingBodySize, body.Length);

            await using var channel = await channelFactory(cancellationToken);

            try
            {
                await channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: queueName,
                    mandatory: true,
                    basicProperties: properties,
                    body: body,
                    cancellationToken: cancellationToken);
            }
            catch (PublishReturnException returned)
            {
                throw new UnroutableMessageException(
                    ErrorMessages.SendToQueueUnroutable(queueName, returned.ReplyCode, returned.ReplyText),
                    returned)
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
