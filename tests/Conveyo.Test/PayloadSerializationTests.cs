using System.Text.Json;
using System.Text.Json.Nodes;
using Conveyo.Serialization;

namespace Conveyo.Test;

[TestFixture]
public class PayloadSerializationTests
{
    [Test]
    public void Deserialize_SharedNumericAndBinaryExample_PreservesConcreteValues()
    {
        // Arrange
        var json = ReadExample("types.json");

        // Act
        var payload = JsonSerializer.Deserialize<PayloadExample>(json, ConveyoJsonOptions.Default);

        // Assert
        payload.ShouldNotBeNull();
        payload.Sequence.ShouldBe(long.MaxValue);
        payload.Amount.ShouldBe(1234567890.125m);
        payload.Kind.ShouldBe(SampleKind.Humidity);
        payload.Note.ShouldBeNull();
        payload.Readings.ShouldBe(new decimal[] { 1, 2.5m, -3 });
        payload.Binary.ShouldBe(new byte[] { 0, 1, 254, 255 });
    }

    [Test]
    public void Serialize_NumericAndBinaryPayload_MatchesSharedExample()
    {
        // Arrange
        var payload = new PayloadExample(long.MaxValue, 1234567890.125m,
            SampleKind.Humidity, null, [1, 2.5m, -3], [0, 1, 254, 255]);
        var example = JsonNode.Parse(ReadExample("types.json"));

        // Act
        var wire = JsonSerializer.SerializeToNode(payload, ConveyoJsonOptions.Default);

        // Assert
        JsonNode.DeepEquals(wire, example).ShouldBeTrue($"Expected {example}; actual {wire}");
    }

    private static string ReadExample(string file) => File.ReadAllText(Path.Combine(
        TestContext.CurrentContext.TestDirectory, "contracts", "fixtures", "payloads", file));
}

internal enum SampleKind
{
    Temperature,
    Humidity
}

internal sealed record PayloadExample(
    long Sequence, decimal Amount, SampleKind Kind, string? Note, decimal[] Readings, byte[] Binary);
