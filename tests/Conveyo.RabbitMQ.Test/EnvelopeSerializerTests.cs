using System.Text.Json;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class EnvelopeSerializerTests
{
    private sealed record SampleMessage(string Name, int Count);

    private const string SampleUrn = "conveyo:test.sample.v1";

    [Test]
    public void Create_PopulatesMessageIdTypeAndSentTime()
    {
        var hostInfo = new HostInfo { MachineName = "test-host" };
        var message = new SampleMessage("demo", 3);

        var envelope = EnvelopeSerializer.Create(message, hostInfo, SampleUrn);

        Assert.That(envelope.EnvelopeVersion, Is.EqualTo(MessageEnvelope.CurrentEnvelopeVersion));
        Assert.That(envelope.MessageId, Is.Not.Null.And.Not.EqualTo(Guid.Empty));
        Assert.That(envelope.MessageType, Is.EqualTo(new[] { SampleUrn }));
        Assert.That(envelope.Host, Is.SameAs(hostInfo));
        Assert.That(envelope.SentTime, Is.Not.Null);
        Assert.That(envelope.Message.ValueKind, Is.EqualTo(JsonValueKind.Object));
        Assert.That(envelope.Message.GetProperty("name").GetString(), Is.EqualTo("demo"));
        Assert.That(envelope.Message.GetProperty("count").GetInt32(), Is.EqualTo(3));
    }

    [Test]
    public void Serialize_IncludesEnvelopeVersion()
    {
        var envelope = EnvelopeSerializer.Create(new SampleMessage("v", 1), new HostInfo(), SampleUrn);

        var json = System.Text.Encoding.UTF8.GetString(EnvelopeSerializer.Serialize(envelope));

        using var document = JsonDocument.Parse(json);
        Assert.That(document.RootElement.TryGetProperty("envelopeVersion", out var version), Is.True);
        Assert.That(version.GetString(), Is.EqualTo("1"));
    }

    [Test]
    public void RoundTrip_PreservesEnvelopeVersion()
    {
        var envelope = EnvelopeSerializer.Create(new SampleMessage("v", 1), new HostInfo(), SampleUrn);

        var bytes = EnvelopeSerializer.Serialize(envelope);
        var restored = EnvelopeSerializer.Deserialize(bytes);

        Assert.That(restored, Is.Not.Null);
        Assert.That(restored!.EnvelopeVersion, Is.EqualTo(envelope.EnvelopeVersion));
    }

    [Test]
    public void RoundTrip_PreservesMessageFields()
    {
        var hostInfo = new HostInfo { MachineName = "origin" };
        var envelope = EnvelopeSerializer.Create(new SampleMessage("round", 42), hostInfo, SampleUrn);

        var bytes = EnvelopeSerializer.Serialize(envelope);
        var restored = EnvelopeSerializer.Deserialize(bytes);

        Assert.That(restored, Is.Not.Null);
        Assert.That(restored!.MessageId, Is.EqualTo(envelope.MessageId));
        Assert.That(restored.MessageType, Is.EqualTo(envelope.MessageType));
        Assert.That(restored.Host!.MachineName, Is.EqualTo("origin"));

        Assert.That(restored.Message.ValueKind, Is.EqualTo(JsonValueKind.Object));
        Assert.That(restored.Message.GetProperty("name").GetString(), Is.EqualTo("round"));
        Assert.That(restored.Message.GetProperty("count").GetInt32(), Is.EqualTo(42));

        var typed = restored.Message.Deserialize<SampleMessage>(Conveyo.Serialization.ConveyoJsonOptions.Default);
        Assert.That(typed, Is.EqualTo(new SampleMessage("round", 42)));
    }

    [Test]
    public void Deserialize_RejectsUnsupportedEnvelopeVersion()
    {
        var envelope = EnvelopeSerializer.Create(new SampleMessage("v", 1), new HostInfo(), SampleUrn) with
        {
            EnvelopeVersion = "2"
        };

        var ex = Assert.Throws<EnvelopeDeserializationException>(
            () => EnvelopeSerializer.Deserialize(EnvelopeSerializer.Serialize(envelope)));
        Assert.That(ex!.Message, Does.Contain("Unsupported envelope version"));
    }

    [Test]
    public void Deserialize_RejectsMissingMessageType()
    {
        var json = "{\"envelopeVersion\":\"1\",\"message\":{\"name\":\"x\",\"count\":1}}";
        var ex = Assert.Throws<EnvelopeDeserializationException>(
            () => EnvelopeSerializer.Deserialize(System.Text.Encoding.UTF8.GetBytes(json)));
        Assert.That(ex!.Message, Does.Contain("messageType"));
    }

    [Test]
    public void Deserialize_RejectsNullMessage()
    {
        var json = "{\"envelopeVersion\":\"1\",\"messageType\":[\"" + SampleUrn + "\"],\"message\":null}";
        var ex = Assert.Throws<EnvelopeDeserializationException>(
            () => EnvelopeSerializer.Deserialize(System.Text.Encoding.UTF8.GetBytes(json)));
        Assert.That(ex!.Message, Does.Contain("'message'"));
    }

    [Test]
    public void Create_PropagatesAmbientCorrelationAndHeaders()
    {
        var correlationId = Guid.NewGuid();
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["tenant-id"] = "tenant-42",
            ["priority"] = "9"
        };

        MessageEnvelope envelope;
        using (Conveyo.OutboundContext.Push(new OutboundMetadata(correlationId, headers)))
        {
            envelope = EnvelopeSerializer.Create(new SampleMessage("hi", 1), new HostInfo(), SampleUrn);
        }

        Assert.That(envelope.CorrelationId, Is.EqualTo(correlationId));
        Assert.That(envelope.Headers, Is.Not.Null);
        Assert.That(envelope.Headers!["tenant-id"], Is.EqualTo("tenant-42"));
        Assert.That(envelope.Headers["priority"], Is.EqualTo("9"));
    }

    [Test]
    public void Serialize_IsNotIndented()
    {
        var envelope = EnvelopeSerializer.Create(new SampleMessage("x", 1), new HostInfo(), SampleUrn);

        var json = System.Text.Encoding.UTF8.GetString(EnvelopeSerializer.Serialize(envelope));

        // Indented output would contain newlines; the transport deliberately emits compact JSON.
        Assert.That(json, Does.Not.Contain("\n"));
    }
}
