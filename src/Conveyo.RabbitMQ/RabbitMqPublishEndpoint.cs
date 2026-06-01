using System.Diagnostics;
using Conveyo.Diagnostics;
using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqPublishEndpoint(
    Func<CancellationToken, Task<IChannel>> channelFactory,
    string exchangeName,
    HostInfo hostInfo,
    string urn,
    Func<IChannel, string, CancellationToken, Task>? ensureExchangeDeclaredAsync = null) : IPublishEndpoint
{
    public async Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        var envelope = EnvelopeSerializer.Create(message, hostInfo, urn);
        var body = EnvelopeSerializer.Serialize(envelope);

        using var activity = ConveyoActivitySource.StartProducer(
            RabbitMqDiagnosticHeaders.MessagingSystem,
            "publish",
            exchangeName,
            envelope);
        try
        {
            var properties = RabbitMqMessageProperties.ForEnvelope(envelope);
            RabbitMqTraceContextPropagation.Inject(activity, properties.Headers!);
            activity?.SetTag(RabbitMqDiagnosticHeaders.RoutingKey, string.Empty);
            activity?.SetTag(DiagnosticHeaders.MessagingBodySize, body.Length);

            await using var channel = await channelFactory(cancellationToken);

            if (ensureExchangeDeclaredAsync is not null)
            {
                await ensureExchangeDeclaredAsync(channel, exchangeName, cancellationToken);
            }

            // mandatory:false is intentional - publishes go to a fanout exchange so the message is
            // delivered to every bound queue, but having zero bindings is not a failure for events.
            // Publisher confirms (enabled on the channel) ensure the broker has accepted the message
            // durably before this await returns.
            await channel.BasicPublishAsync(
                exchange: exchangeName,
                routingKey: string.Empty,
                mandatory: false,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }
}
