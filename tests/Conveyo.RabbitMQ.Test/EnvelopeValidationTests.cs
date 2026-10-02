using System.Text;
using System.Text.Json;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class EnvelopeValidationTests
{
    [TestCase("""{"envelopeVersion":null,"messageType":["example:payload.v1"],"message":{}}""")]
    [TestCase("""{"envelopeVersion":"","messageType":["example:payload.v1"],"message":{}}""")]
    [TestCase("""{"envelopeVersion":1,"messageType":["example:payload.v1"],"message":{}}""")]
    public void Deserialize_RejectsInvalidVersionValue(string json)
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes(json);

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [TestCase("""{"envelopeVersion":"1","messageType":null,"message":{}}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":[],"message":{}}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":[""],"message":{}}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":[null],"message":{}}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["bad urn"],"message":{}}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["é"],"message":{}}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["urn\n"],"message":{}}""")]
    public void Deserialize_RejectsInvalidMessageUrns(string json)
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes(json);

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsUrnLongerThan255Bytes()
    {
        // Arrange
        var urn = new string('a', 256);
        var body = Encoding.UTF8.GetBytes($$"""
            {"envelopeVersion":"1","messageType":["{{urn}}"],"message":{} }
            """);

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":[]}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":1}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":"text"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":true}""")]
    public void Deserialize_RejectsNonObjectPayload(string json)
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes(json);

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"messageId":"not-uuid"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"messageId":42}""")]
    public void Deserialize_RejectsInvalidMessageId(string json)
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes(json);

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsInvalidCorrelationId()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"correlationId":"123"}""");

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"yesterday"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"2026-02-30T00:00:00Z"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"2026-05-14T12:34:56"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"2026-05-14T12:34:56+02:00"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"2026-05-14T12:34:56.12345678901234567Z"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"2026-05-14t12:34:56Z"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"2026-05-14T12:34:56z"}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"2026-05-14T12:34:56Z\n"}""")]
    public void Deserialize_RejectsInvalidTimestamp(string json)
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes(json);

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"headers":{"a":null}}""")]
    [TestCase("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"headers":{"a":1}}""")]
    public void Deserialize_RejectsNonStringHeaders(string json)
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes(json);

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsNonObjectHost()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"host":"host"}""");

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsRelativeDestination()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("""{"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"destinationAddress":"relative"}""");

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsMissingVersion()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("""{"messageType":["example:payload.v1"],"message":{}}""");

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsMissingPayload()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("""{"envelopeVersion":"1","messageType":["example:payload.v1"]}""");

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsNullRoot()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("""null""");

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsArrayRoot()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("""[]""");

        // Act
        var deserialize = () => EnvelopeSerializer.Deserialize(body);

        // Assert
        Should.Throw<EnvelopeDeserializationException>(deserialize);
    }

    [Test]
    public void Deserialize_IgnoresUnknownEnvelopeAndHostFields()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("""
            {"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},
             "future":{"enabled":true},"host":{"runtime":"other","future":42}}
            """);

        // Act
        var envelope = EnvelopeSerializer.Deserialize(body);

        // Assert
        envelope.Host.ShouldNotBeNull();
        envelope.Host.Runtime.ShouldBe("other");
        envelope.Message.ValueKind.ShouldBe(JsonValueKind.Object);
    }

    [TestCase("2026-05-14T12:34:56Z", "2026-05-14T12:34:56Z")]
    [TestCase("2026-05-14T12:34:56.1Z", "2026-05-14T12:34:56.1Z")]
    [TestCase("2026-05-14T12:34:56.1234567Z", "2026-05-14T12:34:56.1234567Z")]
    [TestCase("2026-05-14T12:34:56.123456789Z", "2026-05-14T12:34:56.1234567Z")]
    [TestCase("2026-05-14T12:34:56.1234567890123456Z", "2026-05-14T12:34:56.1234567Z")]
    public void Deserialize_AcceptsUtcTimestampWithUpToSixteenFractionalDigits(string sentTime, string expectedTime)
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes($$"""
            {"envelopeVersion":"1","messageType":["example:payload.v1"],"message":{},"sentTime":"{{sentTime}}"}
            """);

        // Act
        var envelope = EnvelopeSerializer.Deserialize(body);

        // Assert
        envelope.SentTime.ShouldNotBeNull();
        envelope.SentTime.Value.Kind.ShouldBe(DateTimeKind.Utc);
        // DateTime retains at most seven fractional digits.
        envelope.SentTime.Value.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF'Z'").ShouldBe(
            expectedTime);
    }

    [TestCase("types.json", "example:payload.v1")]
    [TestCase("fault.json", "example:payload.v1.fault")]
    public void Deserialize_SharedPayloadExample_AcceptsObjectPayload(string file, string urn)
    {
        // Arrange
        var payload = File.ReadAllText(Path.Combine(TestContext.CurrentContext.TestDirectory,
            "contracts", "fixtures", "payloads", file));
        var body = Encoding.UTF8.GetBytes($$"""
            {"envelopeVersion":"1","messageType":["{{urn}}"],"message":{{payload}}}
            """);

        // Act
        var envelope = EnvelopeSerializer.Deserialize(body);

        // Assert
        envelope.MessageType.ShouldBe(new[] { urn });
        envelope.Message.ValueKind.ShouldBe(JsonValueKind.Object);
        System.Text.Json.Nodes.JsonNode.Parse(envelope.Message.GetRawText()).ShouldBeJson(payload);
    }
}
