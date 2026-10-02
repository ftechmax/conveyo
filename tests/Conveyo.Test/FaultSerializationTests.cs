using System.Text.Json;
using System.Text.Json.Nodes;
using Conveyo.Serialization;

namespace Conveyo.Test;

[TestFixture]
public class FaultSerializationTests
{
    [Test]
    public void Deserialize_SharedFaultExample_ReadsIdentityTimestampHostAndOriginalPayload()
    {
        // Arrange
        var json = File.ReadAllText(Path.Combine(TestContext.CurrentContext.TestDirectory,
            "contracts", "fixtures", "payloads", "fault.json"));

        // Act
        var fault = JsonSerializer.Deserialize<Fault<PayloadExample>>(json, ConveyoJsonOptions.Default);

        // Assert
        fault.ShouldNotBeNull();
        fault.FaultId.ShouldBe(Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
        fault.FaultedMessageId.ShouldBe(Guid.Parse("11111111-2222-3333-4444-555555555555"));
        fault.Timestamp.ShouldBe(new DateTime(2026, 5, 14, 12, 34, 56, 789, DateTimeKind.Utc));
        fault.Timestamp.Kind.ShouldBe(DateTimeKind.Utc);
        fault.Host.MachineName.ShouldBe("golden-host");
        fault.Host.Runtime.ShouldBe("dotnet");
        fault.Exceptions.Length.ShouldBe(1);
        fault.Exceptions[0].ShouldBe(new ExceptionInfo
        {
            ExceptionType = "System.InvalidOperationException",
            Message = "Exception details redacted."
        });
        fault.Message.Sequence.ShouldBe(long.MaxValue);
        fault.Message.Amount.ShouldBe(1234567890.125m);
        fault.Message.Kind.ShouldBe(SampleKind.Humidity);
        fault.Message.Note.ShouldBeNull();
        fault.Message.Readings.ShouldBe(new decimal[] { 1, 2.5m, -3 });
        fault.Message.Binary.ShouldBe(new byte[] { 0, 1, 254, 255 });
    }

    [Test]
    public void Serialize_DefaultExceptionInfo_MatchesSharedRedactedExample()
    {
        // Arrange
        var example = JsonNode.Parse(File.ReadAllText(Path.Combine(TestContext.CurrentContext.TestDirectory,
            "contracts", "fixtures", "payloads", "fault.json")))!;
        var exception = new InvalidOperationException("private", new Exception("inner"));

        // Act
        var wire = JsonSerializer.SerializeToNode(ExceptionInfo.From(exception), ConveyoJsonOptions.Default);

        // Assert
        JsonNode.DeepEquals(wire, example["exceptions"]![0]).ShouldBeTrue(
            $"Expected {example["exceptions"]![0]}; actual {wire}");
    }
}
