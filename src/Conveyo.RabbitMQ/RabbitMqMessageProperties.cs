using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

internal static class RabbitMqMessageProperties
{
    public const string ConveyoVersionHeader = "conveyo-version";

    public static BasicProperties PersistentJson() => new()
    {
        ContentType = "application/json",
        Persistent = true
    };

    public static BasicProperties ForEnvelope(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var properties = PersistentJson();

        if (envelope.MessageId.HasValue)
        {
            properties.MessageId = envelope.MessageId.Value.ToString();
        }

        if (envelope.CorrelationId.HasValue)
        {
            properties.CorrelationId = envelope.CorrelationId.Value.ToString();
        }

        if (envelope.MessageType is { Length: > 0 } messageTypes)
        {
            properties.Type = messageTypes[0];
        }

        if (envelope.SentTime.HasValue)
        {
            var sentUtc = DateTime.SpecifyKind(envelope.SentTime.Value, DateTimeKind.Utc);
            var unixSeconds = new DateTimeOffset(sentUtc).ToUnixTimeSeconds();
            properties.Timestamp = new AmqpTimestamp(unixSeconds);
        }

        properties.Headers = new Dictionary<string, object?>
        {
            [ConveyoVersionHeader] = envelope.EnvelopeVersion
        };

        return properties;
    }
}
