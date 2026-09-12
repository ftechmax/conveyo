using System.Text.Json;
using Conveyo.Serialization;

namespace Conveyo.RabbitMQ;

internal static class EnvelopeSerializer
{
    public static byte[] Serialize(MessageEnvelope envelope) =>
        JsonSerializer.SerializeToUtf8Bytes(envelope, ConveyoJsonOptions.Default);

    public static MessageEnvelope Deserialize(ReadOnlySpan<byte> body)
    {
        MessageEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<MessageEnvelope>(body, ConveyoJsonOptions.Default);
        }
        catch (JsonException ex)
        {
            throw new EnvelopeDeserializationException(ErrorMessages.MessageEnvelopeJsonInvalid, ex);
        }

        if (envelope is null)
        {
            throw new EnvelopeDeserializationException(ErrorMessages.MessageEnvelopeDeserializedToNull);
        }

        ValidateContract(envelope);

        return envelope;
    }

    public static MessageEnvelope Create<T>(T message, HostInfo hostInfo, string urn) where T : class
    {
        ArgumentException.ThrowIfNullOrEmpty(urn);

        var outbound = OutboundContext.Current;

        var envelope = new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = outbound?.CorrelationId,
            MessageType = [urn],
            Message = JsonSerializer.SerializeToElement(message, ConveyoJsonOptions.Default),
            SentTime = DateTime.UtcNow,
            Host = hostInfo,
            Headers = CopyHeaders(outbound?.Headers)
        };
        ValidateContract(envelope);
        return envelope;
    }

    private static Dictionary<string, string>? CopyHeaders(IReadOnlyDictionary<string, string>? source)
    {
        if (source is null || source.Count == 0)
        {
            return null;
        }

        var copy = new Dictionary<string, string>(source.Count, StringComparer.Ordinal);
        foreach (var kvp in source)
        {
            copy[kvp.Key] = kvp.Value;
        }

        return copy;
    }

    private static void ValidateContract(MessageEnvelope envelope)
    {
        if (string.IsNullOrEmpty(envelope.EnvelopeVersion))
        {
            throw new EnvelopeDeserializationException(ErrorMessages.MissingEnvelopeVersion);
        }

        if (!string.Equals(envelope.EnvelopeVersion, MessageEnvelope.CurrentEnvelopeVersion, StringComparison.Ordinal))
        {
            throw new EnvelopeDeserializationException(ErrorMessages.UnsupportedEnvelopeVersion(envelope.EnvelopeVersion));
        }

        if (envelope.MessageType is null || envelope.MessageType.Length == 0
            || envelope.MessageType.Any(string.IsNullOrEmpty))
        {
            throw new EnvelopeDeserializationException(ErrorMessages.MissingMessageType);
        }

        try
        {
            foreach (var urn in envelope.MessageType)
            {
                ConveyoContext.ValidateUrn(urn);
            }
        }
        catch (ArgumentException ex)
        {
            throw new EnvelopeDeserializationException("Envelope contains an invalid message URN.", ex);
        }

        if (envelope.Headers?.Values.Any(value => value is null) == true)
        {
            throw new EnvelopeDeserializationException("Envelope application headers must have string values.");
        }

        if (envelope.SentTime is { Kind: not DateTimeKind.Utc })
        {
            throw new EnvelopeDeserializationException("Envelope 'sentTime' must be UTC with a Z suffix.");
        }

        if (envelope.DestinationAddress is { IsAbsoluteUri: false })
        {
            throw new EnvelopeDeserializationException("Envelope 'destinationAddress' must be an absolute URI.");
        }

        if (envelope.Message.ValueKind != JsonValueKind.Object)
        {
            throw new EnvelopeDeserializationException(ErrorMessages.MissingMessage);
        }
    }
}
