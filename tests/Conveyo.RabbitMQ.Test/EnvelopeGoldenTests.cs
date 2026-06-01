using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
internal class EnvelopeGoldenTests
{
    private static readonly JsonSerializerOptions IndentedJsonOptions = new()
    {
        WriteIndented = true
    };

    private static readonly JsonSerializerOptions CompactJsonOptions = new();

    public static IEnumerable<TestCaseData> Cases() =>
        GoldenCases().Select(testCase =>
            new TestCaseData(testCase.FileName, testCase.Envelope).SetName(testCase.Name));

    private static IEnumerable<GoldenCase> GoldenCases()
    {
        yield return new GoldenCase(
            "plain-command.json",
            CreateEnvelope(
                ["conveyo:golden.submit-invoice.v1"],
                new SubmitInvoiceCommand("INV-2026-0001", 3, true)),
            "Plain command envelope with primitive properties");

        yield return new GoldenCase(
            "plain-event-nested-list.json",
            CreateEnvelope(
                ["conveyo:golden.invoice-posted.v1"],
                new InvoicePostedEvent(
                    "INV-2026-0001",
                    new CustomerSnapshot("CUST-42", "Ada Lovelace"),
                    [
                        new InvoiceLine("consulting", 2, 125.50m),
                        new InvoiceLine("support", 1, 75.00m)
                    ])),
            "Plain event envelope with nested object and list");

        yield return new GoldenCase(
            "message-data-inline.json",
            CreateEnvelope(
                ["conveyo:golden.inline-payload.v1"],
                new PayloadAcceptedCommand(
                    "inline-payload",
                    new MessageData<string>(new Uri("data:text/plain;base64,U21hbGwgcGF5bG9hZA==")))),
            "Envelope carrying MessageData string inline URI");

        yield return new GoldenCase(
            "message-data-pgbin.json",
            CreateEnvelope(
                ["conveyo:golden.pgbin-payload.v1"],
                new PayloadAcceptedCommand(
                    "pgbin-payload",
                    new MessageData<string>(new Uri("pgbin://message_data/files/0194ad8f-61a2-7f28-9001-111111111111")))),
            "Envelope carrying MessageData string pgbin URI");

        yield return new GoldenCase(
            "headers.json",
            CreateEnvelope(
                ["conveyo:golden.headered-command.v1"],
                new SubmitInvoiceCommand("INV-2026-0002", 1, false),
                headers: new Dictionary<string, string>
                {
                    ["tenant-id"] = "tenant-007",
                    ["priority"] = "5",
                    ["trace-enabled"] = "true",
                    ["source"] = "golden-tests"
                }),
            "Envelope with non-default headers");

        yield return new GoldenCase(
            "message-type-two-urns.json",
            CreateEnvelope(
                ["conveyo:golden.invoice-posted.v2", "conveyo:golden.invoice-posted"],
                new InvoicePostedV2Event(
                    "INV-2026-0003",
                    "posted",
                    new CustomerSnapshot("CUST-77", "Grace Hopper"))),
            "Envelope with two MessageType URNs");
    }

    [TestCaseSource(nameof(Cases))]
    public void Serialize_MatchesGoldenEnvelope(string goldenFileName, MessageEnvelope envelope)
    {
        var actual = FormatForGoldenFile(EnvelopeSerializer.Serialize(envelope));
        var goldenPath = GoldenPath(goldenFileName);

        var expected = NormalizeLineEndings(File.ReadAllText(goldenPath, Encoding.UTF8));
        if (actual == expected)
        {
            return;
        }

        Assert.Fail(BuildFailureMessage(goldenFileName, expected, actual));
    }

    private static MessageEnvelope CreateEnvelope(
        string[] messageType,
        object message,
        Dictionary<string, string>? headers = null)
    {
        return new MessageEnvelope
        {
            MessageId = Guid.Parse("11111111-2222-3333-4444-555555555555"),
            CorrelationId = Guid.Parse("33333333-4444-5555-6666-777777777777"),
            DestinationAddress = new Uri("queue:golden-destination"),
            MessageType = messageType,
            Message = JsonSerializer.SerializeToElement(message, Conveyo.Serialization.ConveyoJsonOptions.Default),
            SentTime = DateTimeOffset.Parse("2026-05-14T12:34:56.7890000Z").UtcDateTime,
            Headers = headers,
            Host = new HostInfo
            {
                MachineName = "golden-host",
                ProcessName = "Conveyo.GoldenTests",
                ConveyoVersion = "0.0.0-golden",
                OperatingSystemVersion = "Unix 6.1.0",
                Runtime = "dotnet",
                RuntimeVersion = "10.0.0"
            }
        };
    }

    private static string FormatForGoldenFile(byte[] serializedEnvelope)
    {
        var node = JsonNode.Parse(serializedEnvelope)
            ?? throw new InvalidOperationException("EnvelopeSerializer emitted empty JSON.");

        return NormalizeLineEndings(node.ToJsonString(IndentedJsonOptions)) + "\n";
    }

    private static string GoldenPath(string goldenFileName)
    {
        var testDirectory = TestContext.CurrentContext.TestDirectory;
        return Path.GetFullPath(Path.Combine(testDirectory, "..", "..", "..", "GoldenEnvelopes", goldenFileName));
    }

    private static string BuildFailureMessage(string goldenFileName, string expected, string actual)
    {
        var expectedNode = JsonNode.Parse(expected);
        var actualNode = JsonNode.Parse(actual);
        var diff = FindFirstDifference(expectedNode, actualNode, "$")
            ?? "JSON values match, but formatting or property order changed.";

        return $"""
            Golden envelope drifted: {goldenFileName}
            {diff}
            If this is an intentional wire contract change, hand-edit the golden JSON and review the diff.
            """;
    }

    private static string? FindFirstDifference(JsonNode? expected, JsonNode? actual, string path)
    {
        if (JsonNode.DeepEquals(expected, actual))
        {
            return null;
        }

        if (expected is null || actual is null)
        {
            return $"{path}: expected {ToCompactJson(expected)}, actual {ToCompactJson(actual)}";
        }

        if (expected.GetType() != actual.GetType())
        {
            return $"{path}: expected {expected.GetType().Name}, actual {actual.GetType().Name}";
        }

        if (expected is JsonObject expectedObject && actual is JsonObject actualObject)
        {
            foreach (var property in expectedObject)
            {
                if (!actualObject.ContainsKey(property.Key))
                {
                    return $"{path}.{property.Key}: missing from actual JSON";
                }

                var diff = FindFirstDifference(property.Value, actualObject[property.Key], $"{path}.{property.Key}");
                if (diff is not null)
                {
                    return diff;
                }
            }

            foreach (var property in actualObject)
            {
                if (!expectedObject.ContainsKey(property.Key))
                {
                    return $"{path}.{property.Key}: unexpected field in actual JSON";
                }
            }

            return null;
        }

        if (expected is JsonArray expectedArray && actual is JsonArray actualArray)
        {
            if (expectedArray.Count != actualArray.Count)
            {
                return $"{path}: expected array length {expectedArray.Count}, actual {actualArray.Count}";
            }

            for (var i = 0; i < expectedArray.Count; i++)
            {
                var diff = FindFirstDifference(expectedArray[i], actualArray[i], $"{path}[{i}]");
                if (diff is not null)
                {
                    return diff;
                }
            }

            return null;
        }

        return $"{path}: expected {ToCompactJson(expected)}, actual {ToCompactJson(actual)}";
    }

    private static string ToCompactJson(JsonNode? node) =>
        node?.ToJsonString(CompactJsonOptions) ?? "null";

    private static string NormalizeLineEndings(string value) =>
        value.Replace("\r\n", "\n", StringComparison.Ordinal).Replace("\r", "\n", StringComparison.Ordinal);

    private sealed record GoldenCase(string FileName, MessageEnvelope Envelope, string Name);

    private sealed record SubmitInvoiceCommand(string InvoiceNumber, int RetryCount, bool Force);

    private sealed record InvoicePostedEvent(
        string InvoiceNumber,
        CustomerSnapshot Customer,
        IReadOnlyList<InvoiceLine> Lines);

    private sealed record CustomerSnapshot(string CustomerId, string DisplayName);

    private sealed record InvoiceLine(string Sku, int Quantity, decimal UnitPrice);

    private sealed record PayloadAcceptedCommand(string Name, MessageData<string> Payload);

    private sealed record InvoicePostedV2Event(
        string InvoiceNumber,
        string Status,
        CustomerSnapshot Customer);
}
