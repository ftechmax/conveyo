namespace Conveyo.Test;

[TestFixture]
public class DataUriTests
{
    [Test]
    public void Decode_Base64_RoundTripsArbitraryBytes()
    {
        // Arrange
        var payload = new byte[] { 0x00, 0x01, 0xFF, 0x7F, 0x80, 0xFE };
        var uri = new Uri("data:application/octet-stream;base64," + Convert.ToBase64String(payload));

        // Act
        using var stream = DataUri.Decode(uri);
        using var memory = new MemoryStream();
        stream.CopyTo(memory);

        // Assert
        memory.ToArray().ShouldBe(payload);
    }

    [TestCase("data:application/octet-stream;BASE64,AQID", new byte[] { 1, 2, 3 })]
    [TestCase("DATA:text/plain;BASE64,aGk=", new byte[] { 104, 105 })]
    public void Decode_Base64Parameter_IsCaseInsensitive(string address, byte[] expected)
    {
        // Arrange
        var uri = new Uri(address);

        // Act
        using var stream = DataUri.Decode(uri);
        using var memory = new MemoryStream();
        stream.CopyTo(memory);

        // Assert
        memory.ToArray().ShouldBe(expected);
    }

    [TestCase("data:;base64,AQID", new byte[] { 1, 2, 3 })]
    [TestCase("data:;base64,AAH+/w==", new byte[] { 0, 1, 254, 255 })]
    public void Decode_Base64Parameter_MayOmitMediaType(string address, byte[] expected)
    {
        // Arrange
        var uri = new Uri(address);

        // Act
        using var stream = DataUri.Decode(uri);
        using var memory = new MemoryStream();
        stream.CopyTo(memory);

        // Assert
        memory.ToArray().ShouldBe(expected);
    }

    [Test]
    public void Decode_Base64_RejectsPayloadAboveLimitBeforeHydration()
    {
        // Arrange
        var uri = new Uri("data:application/octet-stream;base64," + Convert.ToBase64String([1, 2, 3, 4, 5]));

        // Act
        var ex = Should.Throw<InvalidDataException>(() => DataUri.Decode(uri, maxBytes: 4));

        // Assert
        ex.Message.ShouldContain("exceeds the configured 4 byte limit");
    }

    [TestCase("data:text/plain,hello%20world")]
    [TestCase("data:text/plain,hello")]
    public void Decode_RejectsNonBase64DataUri(string address)
    {
        // Arrange
        var uri = new Uri(address);

        // Act
        var operation = () => DataUri.Decode(uri);

        // Assert
        Should.Throw<FormatException>(operation);
    }

    [TestCase("data:text/plain;base64ish,AQID")]
    [TestCase("data:;xbase64,aGk=")]
    public void Decode_RejectsLooseBase64MetadataMatch(string address)
    {
        // Arrange
        var uri = new Uri(address);

        // Act
        var operation = () => DataUri.Decode(uri);

        // Assert
        Should.Throw<FormatException>(operation);
    }

    [Test]
    public void Decode_RejectsBase64MediaTypeWithoutBase64Parameter()
    {
        // Arrange
        var uri = new Uri("data:base64," + Convert.ToBase64String([1, 2, 3]));

        // Act
        var operation = () => DataUri.Decode(uri);

        // Assert
        Should.Throw<FormatException>(operation);
    }

    [TestCase("data:text/plain")]
    [TestCase("data:;base64")]
    public void Decode_RejectsMissingCommaSeparator(string address)
    {
        // Arrange
        var uri = new Uri(address);

        // Act
        var operation = () => DataUri.Decode(uri);

        // Assert
        Should.Throw<FormatException>(operation);
    }

    [Test]
    public void Decode_AcceptsText()
    {
        // Arrange
        var address = new Uri("data:text/plain;base64,aGVsbG8=");

        // Act
        using var stream = DataUri.Decode(address);
        using var output = new MemoryStream();
        stream.CopyTo(output);

        // Assert
        output.ToArray().ShouldBe(System.Text.Encoding.UTF8.GetBytes("hello"));
    }

    [Test]
    public void Decode_AcceptsEmptyPayload()
    {
        // Arrange
        var address = new Uri("data:;base64,");

        // Act
        using var stream = DataUri.Decode(address);
        using var output = new MemoryStream();
        stream.CopyTo(output);

        // Assert
        output.ToArray().ShouldBeEmpty();
    }

    [Test]
    public void Decode_RejectsMalformedBase64()
    {
        // Arrange
        var address = new Uri("data:;base64,@@");

        // Act
        var decode = () => DataUri.Decode(address);

        // Assert
        Should.Throw<FormatException>(decode);
    }

    [Test]
    public void Decode_RejectsPercentEncodedBase64()
    {
        // Arrange
        var address = new Uri("data:;base64,aGk%3D");

        // Act
        var decode = () => DataUri.Decode(address);

        // Assert
        Should.Throw<FormatException>(decode);
    }
}
