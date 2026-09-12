using System.Text.Json;
using Conveyo.Serialization;

namespace Conveyo.Test;

[TestFixture]
internal class SharedMessageDataTests
{
    public static IEnumerable<TestCaseData> References() => SharedContracts.Cases("messagedata/references.json");
    public static IEnumerable<TestCaseData> Inline() => SharedContracts.Cases("messagedata/inline.json");

    [TestCaseSource(nameof(References))]
    public void ReferenceAcceptance(JsonElement test)
    {
        var json = test.GetProperty("value").GetRawText();
        var valid = test.GetProperty("valid").GetBoolean();
        if (valid)
        {
            var reference = JsonSerializer.Deserialize<MessageData<byte[]>>(json, ConveyoJsonOptions.Default)!;
            var serialized = JsonSerializer.SerializeToElement(reference, ConveyoJsonOptions.Default);
            Assert.That(serialized.EnumerateObject().Count(), Is.EqualTo(1));
            Assert.That(serialized.GetProperty("address").GetString(), Is.EqualTo(reference.Address.OriginalString));
        }
        else
        {
            Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<MessageData<byte[]>>(json, ConveyoJsonOptions.Default));
        }
    }

    [TestCaseSource(nameof(Inline))]
    public void InlineResolution(JsonElement test)
    {
        var address = new Uri(test.GetProperty("address").GetString()!);
        if (test.GetProperty("outcome").GetString() == "valid")
        {
            using var stream = DataUri.Decode(address);
            using var output = new MemoryStream();
            stream.CopyTo(output);
            Assert.That(Convert.ToHexString(output.ToArray()).ToLowerInvariant(), Is.EqualTo(test.GetProperty("hex").GetString()));
        }
        else
        {
            Assert.Throws<FormatException>(() => DataUri.Decode(address));
        }
    }
}
