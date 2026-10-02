using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Conveyo.Serialization;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class EnvelopeSerializerTests
{
    private sealed record SampleMessage(string Name, int Count);

    private sealed record PayloadValues(string Text, int Count, bool Enabled, decimal Amount, string? Note);

    private sealed record Order(string OrderId, Customer Customer, IReadOnlyList<OrderLine> Lines, IReadOnlyList<string> Tags);

    private sealed record Customer(string CustomerId, string DisplayName);

    private sealed record OrderLine(string Sku, int Quantity);

    private const string SampleUrn = "conveyo:test.sample.v1";

    [Test]
    public void Create_PopulatesMessageIdTypeAndSentTime()
    {
        // Arrange
        var hostInfo = new HostInfo { MachineName = "test-host" };
        var before = DateTime.UtcNow;

        // Act
        var envelope = EnvelopeSerializer.Create(new SampleMessage("demo", 3), hostInfo, SampleUrn);

        // Assert
        envelope.EnvelopeVersion.ShouldBe(MessageEnvelope.CurrentEnvelopeVersion);
        envelope.MessageId.ShouldNotBeNull();
        envelope.MessageId.ShouldNotBe(Guid.Empty);
        envelope.MessageType.ShouldBe(new[] { SampleUrn });
        envelope.Host.ShouldBeSameAs(hostInfo);
        envelope.SentTime.ShouldNotBeNull();
        envelope.SentTime.Value.Kind.ShouldBe(DateTimeKind.Utc);
        envelope.SentTime.Value.ShouldBeInRange(before, DateTime.UtcNow);
    }

    [Test]
    public void Create_SerializesPayloadAsCamelCaseObject()
    {
        // Arrange
        var message = new SampleMessage("demo", 3);

        // Act
        var envelope = EnvelopeSerializer.Create(message, new HostInfo(), SampleUrn);

        // Assert
        envelope.Message.ValueKind.ShouldBe(JsonValueKind.Object);
        envelope.Message.GetProperty("name").GetString().ShouldBe("demo");
        envelope.Message.GetProperty("count").GetInt32().ShouldBe(3);
    }

    [Test]
    public void Create_PropagatesAmbientCorrelationAndHeaders()
    {
        // Arrange
        var correlationId = Guid.NewGuid();
        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["tenant-id"] = "tenant-42",
            ["priority"] = "9"
        };

        // Act
        MessageEnvelope envelope;
        using (OutboundContext.Push(new OutboundMetadata(correlationId, headers)))
        {
            envelope = EnvelopeSerializer.Create(new SampleMessage("hi", 1), new HostInfo(), SampleUrn);
        }

        // Assert
        envelope.CorrelationId.ShouldBe(correlationId);
        envelope.Headers.ShouldNotBeNull();
        envelope.Headers.Count.ShouldBe(2);
        envelope.Headers["tenant-id"].ShouldBe("tenant-42");
        envelope.Headers["priority"].ShouldBe("9");
    }

    [Test]
    public void Create_WithoutAmbientContext_LeavesCorrelationAndHeadersNull()
    {
        // Arrange
        var message = new SampleMessage("hi", 1);

        // Act
        var envelope = EnvelopeSerializer.Create(message, new HostInfo(), SampleUrn);

        // Assert
        envelope.CorrelationId.ShouldBeNull();
        envelope.Headers.ShouldBeNull();
        envelope.DestinationAddress.ShouldBeNull();
    }

    [Test]
    public void Create_RejectsStringPayload()
    {
        // Arrange
        var create = () => EnvelopeSerializer.Create("text", new HostInfo(), SampleUrn);

        // Act
        var exception = Should.Throw<EnvelopeDeserializationException>(create);

        // Assert
        exception.Message.ShouldContain("'message'");
    }

    [Test]
    public void Create_RejectsArrayPayload()
    {
        // Arrange
        var create = () => EnvelopeSerializer.Create(new[] { 1, 2 }, new HostInfo(), SampleUrn);

        // Act
        var exception = Should.Throw<EnvelopeDeserializationException>(create);

        // Assert
        exception.Message.ShouldContain("'message'");
    }

    [Test]
    public void Create_RejectsNullPayload()
    {
        // Arrange
        var create = () => EnvelopeSerializer.Create<SampleMessage>(null!, new HostInfo(), SampleUrn);

        // Act
        var exception = Should.Throw<EnvelopeDeserializationException>(create);

        // Assert
        exception.Message.ShouldContain("'message'");
    }

    [Test]
    public void Serialize_IncludesEnvelopeVersion()
    {
        // Arrange
        var envelope = EnvelopeSerializer.Create(new SampleMessage("v", 1), new HostInfo(), SampleUrn);

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire["envelopeVersion"].ShouldBeJson("\"1\"");
    }

    [Test]
    public void Serialize_IsNotIndented()
    {
        // Arrange
        var envelope = EnvelopeSerializer.Create(new SampleMessage("x", 1), new HostInfo(), SampleUrn);

        // Act
        var json = Encoding.UTF8.GetString(EnvelopeSerializer.Serialize(envelope));

        // Assert
        // Indented output would contain newlines; the transport deliberately emits compact JSON.
        json.ShouldNotContain("\n");
    }

    [Test]
    public void Serialize_WithUnsetOptionalFields_WritesExplicitNulls()
    {
        // Arrange
        var envelope = EnvelopeWith(new SampleMessage("demo", 3));

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire.ShouldBeJson("""
            {
              "envelopeVersion": "1",
              "messageId": null,
              "correlationId": null,
              "destinationAddress": null,
              "messageType": ["conveyo:test.sample.v1"],
              "message": { "name": "demo", "count": 3 },
              "sentTime": null,
              "headers": null,
              "host": null
            }
            """);
    }

    [Test]
    public void Serialize_WithTwoMessageTypeUrns_WritesUrnsInOrder()
    {
        // Arrange
        var envelope = EnvelopeWith(new SampleMessage("demo", 3)) with
        {
            MessageType = ["conveyo:test.sample.v2", "conveyo:test.sample"]
        };

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire["messageType"].ShouldBeJson("""["conveyo:test.sample.v2", "conveyo:test.sample"]""");
    }

    [Test]
    public void Serialize_WithHeaders_WritesStringValuedObject()
    {
        // Arrange
        var envelope = EnvelopeWith(new SampleMessage("demo", 3)) with
        {
            Headers = new Dictionary<string, string>
            {
                ["tenant-id"] = "tenant-007",
                ["priority"] = "5",
                ["trace-enabled"] = "true"
            }
        };

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire["headers"].ShouldBeJson("""{ "tenant-id": "tenant-007", "priority": "5", "trace-enabled": "true" }""");
    }

    [Test]
    public void Serialize_WithMetadata_WritesIdentifiersAndDestinationAsStrings()
    {
        // Arrange
        var envelope = EnvelopeWith(new SampleMessage("demo", 3)) with
        {
            MessageId = Guid.Parse("11111111-2222-3333-4444-555555555555"),
            CorrelationId = Guid.Parse("33333333-4444-5555-6666-777777777777"),
            DestinationAddress = new Uri("queue:sample-queue")
        };

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire["messageId"].ShouldBeJson("\"11111111-2222-3333-4444-555555555555\"");
        wire["correlationId"].ShouldBeJson("\"33333333-4444-5555-6666-777777777777\"");
        wire["destinationAddress"].ShouldBeJson("\"queue:sample-queue\"");
    }

    [Test]
    public void Serialize_WithSentTime_WritesUtcWithoutTrailingZeroFractions()
    {
        // Arrange
        var envelope = EnvelopeWith(new SampleMessage("demo", 3)) with
        {
            SentTime = new DateTime(2026, 5, 14, 12, 34, 56, 789, DateTimeKind.Utc)
        };

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire["sentTime"].ShouldBeJson("\"2026-05-14T12:34:56.789Z\"");
    }

    [Test]
    public void Serialize_WithHostInfo_WritesCamelCaseHostFields()
    {
        // Arrange
        var envelope = EnvelopeWith(new SampleMessage("demo", 3)) with
        {
            Host = new HostInfo { MachineName = "test-host", Runtime = "dotnet", RuntimeVersion = "10.0.0" }
        };

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire["host"].ShouldBeJson("""
            {
              "machineName": "test-host",
              "processName": null,
              "conveyoVersion": null,
              "operatingSystemVersion": null,
              "runtime": "dotnet",
              "runtimeVersion": "10.0.0"
            }
            """);
    }

    [Test]
    public void Serialize_WithPayloadValues_WritesMatchingJsonTypes()
    {
        // Arrange
        var envelope = EnvelopeWith(new PayloadValues("INV-2026-0001", 3, true, 125.50m, null));

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire["message"].ShouldBeJson("""
            { "text": "INV-2026-0001", "count": 3, "enabled": true, "amount": 125.50, "note": null }
            """);
    }

    [Test]
    public void Serialize_WithNestedCollections_WritesNestedObjectsAndOrderedArrays()
    {
        // Arrange
        var order = new Order(
            "ORD-1",
            new Customer("CUST-42", "Ada Lovelace"),
            [new OrderLine("consulting", 2), new OrderLine("support", 1)],
            []);
        var envelope = EnvelopeWith(order);

        // Act
        var wire = SerializeToNode(envelope);

        // Assert
        wire["message"].ShouldBeJson("""
            {
              "orderId": "ORD-1",
              "customer": { "customerId": "CUST-42", "displayName": "Ada Lovelace" },
              "lines": [
                { "sku": "consulting", "quantity": 2 },
                { "sku": "support", "quantity": 1 }
              ],
              "tags": []
            }
            """);
    }

    [Test]
    public void Deserialize_WithOnlyRequiredFields_LeavesOptionalFieldsNull()
    {
        // Arrange
        var body = Utf8("""{"envelopeVersion":"1","messageType":["conveyo:test.sample.v1"],"message":{"name":"demo","count":3}}""");

        // Act
        var envelope = EnvelopeSerializer.Deserialize(body);

        // Assert
        envelope.MessageType.ShouldBe(new[] { SampleUrn });
        envelope.Message.Deserialize<SampleMessage>(ConveyoJsonOptions.Default).ShouldBe(new SampleMessage("demo", 3));
        envelope.MessageId.ShouldBeNull();
        envelope.CorrelationId.ShouldBeNull();
        envelope.DestinationAddress.ShouldBeNull();
        envelope.SentTime.ShouldBeNull();
        envelope.Headers.ShouldBeNull();
        envelope.Host.ShouldBeNull();
    }

    [Test]
    public void Deserialize_WithTwoMessageTypeUrns_PreservesOrder()
    {
        // Arrange
        var body = Utf8("""{"envelopeVersion":"1","messageType":["conveyo:test.sample.v2","conveyo:test.sample"],"message":{"name":"demo","count":3}}""");

        // Act
        var envelope = EnvelopeSerializer.Deserialize(body);

        // Assert
        envelope.MessageType.ShouldBe(new[] { "conveyo:test.sample.v2", "conveyo:test.sample" });
    }

    [Test]
    public void Deserialize_RejectsUnsupportedEnvelopeVersion()
    {
        // Arrange
        var envelope = EnvelopeSerializer.Create(new SampleMessage("v", 1), new HostInfo(), SampleUrn) with
        {
            EnvelopeVersion = "2"
        };
        var body = EnvelopeSerializer.Serialize(envelope);

        // Act
        var exception = Should.Throw<EnvelopeDeserializationException>(() => EnvelopeSerializer.Deserialize(body));

        // Assert
        exception.Message.ShouldContain("Unsupported envelope version");
    }

    [Test]
    public void Deserialize_RejectsMissingMessageType()
    {
        // Arrange
        var body = Utf8("""{"envelopeVersion":"1","message":{"name":"x","count":1}}""");

        // Act
        var exception = Should.Throw<EnvelopeDeserializationException>(() => EnvelopeSerializer.Deserialize(body));

        // Assert
        exception.Message.ShouldContain("messageType");
    }

    [Test]
    public void Deserialize_RejectsNullMessage()
    {
        // Arrange
        var body = Utf8("""{"envelopeVersion":"1","messageType":["conveyo:test.sample.v1"],"message":null}""");

        // Act
        var exception = Should.Throw<EnvelopeDeserializationException>(() => EnvelopeSerializer.Deserialize(body));

        // Assert
        exception.Message.ShouldContain("'message'");
    }

    [Test]
    public void RoundTrip_PreservesEnvelopeVersion()
    {
        // Arrange
        var envelope = EnvelopeSerializer.Create(new SampleMessage("v", 1), new HostInfo(), SampleUrn);

        // Act
        var restored = EnvelopeSerializer.Deserialize(EnvelopeSerializer.Serialize(envelope));

        // Assert
        restored.EnvelopeVersion.ShouldBe(envelope.EnvelopeVersion);
    }

    [Test]
    public void RoundTrip_PreservesMessageFields()
    {
        // Arrange
        var hostInfo = new HostInfo { MachineName = "origin" };
        var envelope = EnvelopeSerializer.Create(new SampleMessage("round", 42), hostInfo, SampleUrn);

        // Act
        var restored = EnvelopeSerializer.Deserialize(EnvelopeSerializer.Serialize(envelope));

        // Assert
        restored.MessageId.ShouldBe(envelope.MessageId);
        restored.MessageType.ShouldBe(envelope.MessageType);
        restored.SentTime.ShouldBe(envelope.SentTime);
        restored.Host.ShouldBe(hostInfo);
        restored.Message.ValueKind.ShouldBe(JsonValueKind.Object);
        restored.Message.Deserialize<SampleMessage>(ConveyoJsonOptions.Default).ShouldBe(new SampleMessage("round", 42));
    }

    [Test]
    public void RoundTrip_PreservesNestedCollections()
    {
        // Arrange
        var order = new Order(
            "ORD-1",
            new Customer("CUST-42", "Ada Lovelace"),
            [new OrderLine("consulting", 2), new OrderLine("support", 1)],
            ["priority", "export"]);
        var envelope = EnvelopeSerializer.Create(order, new HostInfo(), SampleUrn);

        // Act
        var restored = EnvelopeSerializer.Deserialize(EnvelopeSerializer.Serialize(envelope))
            .Message.Deserialize<Order>(ConveyoJsonOptions.Default);

        // Assert
        restored.ShouldNotBeNull();
        restored.OrderId.ShouldBe("ORD-1");
        restored.Customer.ShouldBe(new Customer("CUST-42", "Ada Lovelace"));
        restored.Lines.ShouldBe(new[] { new OrderLine("consulting", 2), new OrderLine("support", 1) });
        restored.Tags.ShouldBe(new[] { "priority", "export" });
    }

    private static MessageEnvelope EnvelopeWith(object message) => new()
    {
        MessageType = [SampleUrn],
        Message = JsonSerializer.SerializeToElement(message, ConveyoJsonOptions.Default)
    };

    private static JsonNode SerializeToNode(MessageEnvelope envelope) =>
        JsonNode.Parse(EnvelopeSerializer.Serialize(envelope))!;

    private static byte[] Utf8(string json) => Encoding.UTF8.GetBytes(json);
}
