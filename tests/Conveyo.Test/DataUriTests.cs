namespace Conveyo.Test;

[TestFixture]
public class DataUriTests
{
    [Test]
    public void Decode_Base64_RoundTripsArbitraryBytes()
    {
        var payload = new byte[] { 0x00, 0x01, 0xFF, 0x7F, 0x80, 0xFE };
        var uri = new Uri("data:application/octet-stream;base64," + Convert.ToBase64String(payload));

        using var stream = DataUri.Decode(uri);
        using var memory = new MemoryStream();
        stream.CopyTo(memory);

        Assert.That(memory.ToArray(), Is.EqualTo(payload));
    }

    [Test]
    public void Decode_Base64Parameter_IsCaseInsensitive()
    {
        var uri = new Uri("data:application/octet-stream;BASE64," + Convert.ToBase64String([1, 2, 3]));

        using var stream = DataUri.Decode(uri);
        using var memory = new MemoryStream();
        stream.CopyTo(memory);

        Assert.That(memory.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public void Decode_Base64Parameter_MayOmitMediaType()
    {
        var uri = new Uri("data:;base64," + Convert.ToBase64String([1, 2, 3]));

        using var stream = DataUri.Decode(uri);
        using var memory = new MemoryStream();
        stream.CopyTo(memory);

        Assert.That(memory.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public void Decode_Base64_RejectsPayloadAboveLimitBeforeHydration()
    {
        var uri = new Uri("data:application/octet-stream;base64," + Convert.ToBase64String([1, 2, 3, 4, 5]));

        var ex = Assert.Throws<InvalidDataException>(() => DataUri.Decode(uri, maxBytes: 4));
        Assert.That(ex!.Message, Does.Contain("exceeds the configured 4 byte limit"));
    }

    [Test]
    public void Decode_RejectsNonBase64DataUri()
    {
        var uri = new Uri("data:text/plain,hello%20world");
        Assert.Throws<FormatException>(() => DataUri.Decode(uri));
    }

    [Test]
    public void Decode_RejectsLooseBase64MetadataMatch()
    {
        var uri = new Uri("data:text/plain;base64ish," + Convert.ToBase64String([1, 2, 3]));
        Assert.Throws<FormatException>(() => DataUri.Decode(uri));
    }

    [Test]
    public void Decode_RejectsBase64MediaTypeWithoutBase64Parameter()
    {
        var uri = new Uri("data:base64," + Convert.ToBase64String([1, 2, 3]));
        Assert.Throws<FormatException>(() => DataUri.Decode(uri));
    }

    [Test]
    public void Decode_RejectsMissingCommaSeparator()
    {
        var uri = new Uri("data:text/plain");
        Assert.Throws<FormatException>(() => DataUri.Decode(uri));
    }
}
