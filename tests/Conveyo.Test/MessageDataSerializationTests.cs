using System.Text.Json;
using Conveyo.Serialization;

namespace Conveyo.Test;

[TestFixture]
public class MessageDataSerializationTests
{
    [TestCase("data:;base64,AAH+/w==")]
    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111")]
    public void RoundTrip_ReferenceSerializesOnlyItsAddress(string address)
    {
        // Arrange
        var json = $$"""{"address":"{{address}}"}""";

        // Act
        var reference = JsonSerializer.Deserialize<MessageData<byte[]>>(json, ConveyoJsonOptions.Default)!;
        var serialized = JsonSerializer.SerializeToElement(reference, ConveyoJsonOptions.Default);

        // Assert
        reference.Address.OriginalString.ShouldBe(address);
        reference.HasValue.ShouldBeFalse();
        serialized.EnumerateObject().Count().ShouldBe(1);
        serialized.GetProperty("address").GetString().ShouldBe(address);
    }

    [Test]
    public void Deserialize_RejectsMissingAddress()
    {
        // Arrange
        var json = "{}";

        // Act
        var deserialize = () => JsonSerializer.Deserialize<MessageData<byte[]>>(json, ConveyoJsonOptions.Default);

        // Assert
        Should.Throw<JsonException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsNullAddress()
    {
        // Arrange
        var json = "{\"address\":null}";

        // Act
        var deserialize = () => JsonSerializer.Deserialize<MessageData<byte[]>>(json, ConveyoJsonOptions.Default);

        // Assert
        Should.Throw<JsonException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsEmptyAddress()
    {
        // Arrange
        var json = "{\"address\":\"\"}";

        // Act
        var deserialize = () => JsonSerializer.Deserialize<MessageData<byte[]>>(json, ConveyoJsonOptions.Default);

        // Assert
        Should.Throw<JsonException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsRelativeAddress()
    {
        // Arrange
        var json = "{\"address\":\"relative\"}";

        // Act
        var deserialize = () => JsonSerializer.Deserialize<MessageData<byte[]>>(json, ConveyoJsonOptions.Default);

        // Assert
        Should.Throw<JsonException>(deserialize);
    }

    [Test]
    public void Deserialize_RejectsStringReference()
    {
        // Arrange
        var json = "\"data:;base64,\"";

        // Act
        var deserialize = () => JsonSerializer.Deserialize<MessageData<byte[]>>(json, ConveyoJsonOptions.Default);

        // Assert
        Should.Throw<JsonException>(deserialize);
    }

}
