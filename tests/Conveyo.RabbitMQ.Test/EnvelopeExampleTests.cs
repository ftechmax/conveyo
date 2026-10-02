using System.Text.Json;
using System.Text.Json.Nodes;
using Conveyo.Serialization;

namespace Conveyo.RabbitMQ.Test;

/// <summary>
/// Checks the envelope examples in <c>contracts/fixtures/envelopes/</c>
/// linked from the wire contract documentation.
/// </summary>
[TestFixture]
internal class EnvelopeExampleTests
{
    private const string CommandExample = "command.json";
    private const string InlineMessageDataExample = "message-data-inline.json";
    private const string PostgresMessageDataExample = "message-data-pgbin.json";

    private const string InlineAddress = "data:text/plain;base64,U21hbGwgcGF5bG9hZA==";
    private const string PostgresAddress = "pgbin://message_data/files/0194ad8f-61a2-7f28-9001-111111111111";

    private static readonly Guid MessageId = Guid.Parse("11111111-2222-3333-4444-555555555555");
    private static readonly Guid CorrelationId = Guid.Parse("33333333-4444-5555-6666-777777777777");
    private static readonly Uri DestinationAddress = new("queue:golden-destination");
    private static readonly DateTime SentTime = new(2026, 5, 14, 12, 34, 56, 789, DateTimeKind.Utc);

    private static readonly HostInfo Host = new()
    {
        MachineName = "golden-host",
        ProcessName = "Conveyo.GoldenTests",
        ConveyoVersion = "0.0.0-golden",
        OperatingSystemVersion = "Unix 6.1.0",
        Runtime = "dotnet",
        RuntimeVersion = "10.0.0"
    };

    [Test]
    public void Serialize_CommandWithMetadataAndNestedPayload_MatchesSharedExample()
    {
        // Arrange
        var command = new SubmitInvoiceCommand(
            "INV-2026-0001",
            new CustomerSnapshot("CUST-42", "Ada Lovelace"),
            [
                new InvoiceLine("consulting", 2, 125.50m),
                new InvoiceLine("support", 1, 75.00m)
            ]);
        var envelope = CreateEnvelope("conveyo:golden.submit-invoice.v1", command) with
        {
            Headers = new Dictionary<string, string>
            {
                ["tenant-id"] = "tenant-007",
                ["priority"] = "5"
            }
        };

        // Act
        var wire = JsonNode.Parse(EnvelopeSerializer.Serialize(envelope));

        // Assert
        wire.ShouldBeJson(ReadExample(CommandExample));
    }

    [Test]
    public void Serialize_InlineMessageData_MatchesSharedExample()
    {
        // Arrange
        var command = new PayloadAcceptedCommand("inline-payload", new MessageData<string>(new Uri(InlineAddress)));
        var envelope = CreateEnvelope("conveyo:golden.inline-payload.v1", command);

        // Act
        var wire = JsonNode.Parse(EnvelopeSerializer.Serialize(envelope));

        // Assert
        wire.ShouldBeJson(ReadExample(InlineMessageDataExample));
    }

    [Test]
    public void Serialize_PostgresMessageData_MatchesSharedExample()
    {
        // Arrange
        var command = new PayloadAcceptedCommand("pgbin-payload", new MessageData<string>(new Uri(PostgresAddress)));
        var envelope = CreateEnvelope("conveyo:golden.pgbin-payload.v1", command);

        // Act
        var wire = JsonNode.Parse(EnvelopeSerializer.Serialize(envelope));

        // Assert
        wire.ShouldBeJson(ReadExample(PostgresMessageDataExample));
    }

    [Test]
    public void Deserialize_CommandExample_ReadsEnvelopeMetadata()
    {
        // Arrange
        var body = ReadExampleBytes(CommandExample);

        // Act
        var envelope = EnvelopeSerializer.Deserialize(body);

        // Assert
        envelope.EnvelopeVersion.ShouldBe("1");
        envelope.MessageId.ShouldBe(MessageId);
        envelope.CorrelationId.ShouldBe(CorrelationId);
        envelope.DestinationAddress.ShouldBe(DestinationAddress);
        envelope.MessageType.ShouldBe(new[] { "conveyo:golden.submit-invoice.v1" });
        envelope.SentTime.ShouldBe(SentTime);
        envelope.SentTime!.Value.Kind.ShouldBe(DateTimeKind.Utc);
        envelope.Host.ShouldBe(Host);
    }

    [Test]
    public void Deserialize_CommandExample_ReadsHeaders()
    {
        // Arrange
        var body = ReadExampleBytes(CommandExample);

        // Act
        var envelope = EnvelopeSerializer.Deserialize(body);

        // Assert
        envelope.Headers.ShouldNotBeNull();
        envelope.Headers.Count.ShouldBe(2);
        envelope.Headers["tenant-id"].ShouldBe("tenant-007");
        envelope.Headers["priority"].ShouldBe("5");
    }

    [Test]
    public void Deserialize_CommandExample_ReadsNestedPayload()
    {
        // Arrange
        var envelope = EnvelopeSerializer.Deserialize(ReadExampleBytes(CommandExample));

        // Act
        var command = envelope.Message.Deserialize<SubmitInvoiceCommand>(ConveyoJsonOptions.Default);

        // Assert
        command.ShouldNotBeNull();
        command.InvoiceNumber.ShouldBe("INV-2026-0001");
        command.Customer.ShouldBe(new CustomerSnapshot("CUST-42", "Ada Lovelace"));
        command.Lines.ShouldBe(new[]
        {
            new InvoiceLine("consulting", 2, 125.50m),
            new InvoiceLine("support", 1, 75.00m)
        });
    }

    [Test]
    public void Deserialize_InlineMessageDataExample_ReadsDataUriReference()
    {
        // Arrange
        var envelope = EnvelopeSerializer.Deserialize(ReadExampleBytes(InlineMessageDataExample));

        // Act
        var command = envelope.Message.Deserialize<PayloadAcceptedCommand>(ConveyoJsonOptions.Default);

        // Assert
        envelope.MessageType.ShouldBe(new[] { "conveyo:golden.inline-payload.v1" });
        envelope.Headers.ShouldBeNull();
        command.ShouldNotBeNull();
        command.Name.ShouldBe("inline-payload");
        command.Payload.Address.ToString().ShouldBe(InlineAddress);
        command.Payload.HasValue.ShouldBeFalse();
    }

    [Test]
    public void Deserialize_PostgresMessageDataExample_ReadsPgbinReference()
    {
        // Arrange
        var envelope = EnvelopeSerializer.Deserialize(ReadExampleBytes(PostgresMessageDataExample));

        // Act
        var command = envelope.Message.Deserialize<PayloadAcceptedCommand>(ConveyoJsonOptions.Default);

        // Assert
        envelope.MessageType.ShouldBe(new[] { "conveyo:golden.pgbin-payload.v1" });
        envelope.Headers.ShouldBeNull();
        command.ShouldNotBeNull();
        command.Name.ShouldBe("pgbin-payload");
        command.Payload.Address.ToString().ShouldBe(PostgresAddress);
        command.Payload.HasValue.ShouldBeFalse();
    }

    [TestCase(CommandExample)]
    [TestCase(InlineMessageDataExample)]
    [TestCase(PostgresMessageDataExample)]
    public void RoundTrip_SharedExample_PreservesEveryJsonValue(string fileName)
    {
        // Arrange
        var example = ReadExample(fileName);
        var envelope = EnvelopeSerializer.Deserialize(ReadExampleBytes(fileName));

        // Act
        var wire = JsonNode.Parse(EnvelopeSerializer.Serialize(envelope));

        // Assert
        wire.ShouldBeJson(example);
    }

    private static MessageEnvelope CreateEnvelope(string urn, object message) => new()
    {
        MessageId = MessageId,
        CorrelationId = CorrelationId,
        DestinationAddress = DestinationAddress,
        MessageType = [urn],
        Message = JsonSerializer.SerializeToElement(message, ConveyoJsonOptions.Default),
        SentTime = SentTime,
        Host = Host
    };

    private static string ReadExample(string fileName) => File.ReadAllText(ExamplePath(fileName));

    private static byte[] ReadExampleBytes(string fileName) => File.ReadAllBytes(ExamplePath(fileName));

    // The project file copies each example next to the test assembly.
    private static string ExamplePath(string fileName) => Path.Combine(
        TestContext.CurrentContext.TestDirectory, "contracts", "fixtures", "envelopes", fileName);

    private sealed record SubmitInvoiceCommand(
        string InvoiceNumber,
        CustomerSnapshot Customer,
        IReadOnlyList<InvoiceLine> Lines);

    private sealed record CustomerSnapshot(string CustomerId, string DisplayName);

    private sealed record InvoiceLine(string Sku, int Quantity, decimal UnitPrice);

    private sealed record PayloadAcceptedCommand(string Name, MessageData<string> Payload);
}
