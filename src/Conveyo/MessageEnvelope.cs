using System.Text.Json;
using System.Text.Json.Serialization;

namespace Conveyo;

internal sealed record MessageEnvelope
{
    public const string CurrentEnvelopeVersion = "1";

    [JsonRequired]
    public string EnvelopeVersion { get; init; } = CurrentEnvelopeVersion;
    public Guid? MessageId { get; init; }
    public Guid? CorrelationId { get; init; }
    public Uri? DestinationAddress { get; init; }
    public string[]? MessageType { get; init; }
    public JsonElement Message { get; init; }
    public DateTime? SentTime { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
    public HostInfo? Host { get; init; }
}
