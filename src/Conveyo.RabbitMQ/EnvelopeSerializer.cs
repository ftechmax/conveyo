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

        return new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = outbound?.CorrelationId,
            MessageType = [urn],
            Message = JsonSerializer.SerializeToElement(message, ConveyoJsonOptions.Default),
            SentTime = DateTime.UtcNow,
            Host = hostInfo,
            Headers = CopyHeaders(outbound?.Headers)
        };
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

        if (envelope.Message.ValueKind == JsonValueKind.Undefined
            || envelope.Message.ValueKind == JsonValueKind.Null)
        {
            throw new EnvelopeDeserializationException(ErrorMessages.MissingMessage);
        }
    }
}
