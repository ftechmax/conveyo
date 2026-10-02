namespace Conveyo.Storage.Postgres.Test;

[TestFixture]
public class PostgresLocatorTests
{
    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    [TestCase("PGBIN://MD/files/0194AD8F-61A2-7F28-9001-111111111111", "MD")]
    [TestCase("pgbin://message_data/files/0194ad8f-61a2-7f28-9001-111111111111", "message_data")]
    [TestCase("pgbin://123/files/0194ad8f-61a2-7f28-9001-111111111111", "123")]
    [TestCase("pgbin://select/files/0194ad8f-61a2-7f28-9001-111111111111", "select")]
    [TestCase("pgbin://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/files/0194ad8f-61a2-7f28-9001-111111111111", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    public void ParseAddress_AcceptsCanonicalLocator(string locator, string schema)
    {
        // Arrange
        var repository = new PostgresMessageDataRepository("", schema);
        var address = new Uri(locator, UriKind.RelativeOrAbsolute);

        // Act
        var id = repository.ParseAddress(address);

        // Assert
        id.ShouldBe(Guid.Parse("0194ad8f-61a2-7f28-9001-111111111111"));
    }

    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111", "other")]
    [TestCase("pgbin://md/chunks/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    [TestCase("pgbin://md/Files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    public void ParseAddress_RejectsDifferentNamespaceOrBucket(string locator, string schema)
    {
        // Arrange
        var repository = new PostgresMessageDataRepository("", schema);
        var address = new Uri(locator, UriKind.RelativeOrAbsolute);

        // Act
        Action parse = () => repository.ParseAddress(address);

        // Assert
        Should.Throw<UnauthorizedAccessException>(parse);
    }

    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111/extra", "md")]
    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111/", "md")]
    [TestCase("pgbin://md//files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    [TestCase("pgbin://md/other/../files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    [TestCase("pgbin://md/%66iles/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    public void ParseAddress_RejectsNonCanonicalPath(string locator, string schema)
    {
        // Arrange
        var repository = new PostgresMessageDataRepository("", schema);
        var address = new Uri(locator, UriKind.RelativeOrAbsolute);

        // Act
        Action parse = () => repository.ParseAddress(address);

        // Assert
        Should.Throw<FormatException>(parse);
    }

    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111?read=1", "md")]
    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111?", "md")]
    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-111111111111#x", "md")]
    public void ParseAddress_RejectsQueryOrFragment(string locator, string schema)
    {
        // Arrange
        var repository = new PostgresMessageDataRepository("", schema);
        var address = new Uri(locator, UriKind.RelativeOrAbsolute);

        // Act
        Action parse = () => repository.ParseAddress(address);

        // Assert
        Should.Throw<FormatException>(parse);
    }

    [TestCase("pgbin://user@md/files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    [TestCase("pgbin://md:5432/files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    public void ParseAddress_RejectsCredentialsOrPort(string locator, string schema)
    {
        // Arrange
        var repository = new PostgresMessageDataRepository("", schema);
        var address = new Uri(locator, UriKind.RelativeOrAbsolute);

        // Act
        Action parse = () => repository.ParseAddress(address);

        // Assert
        Should.Throw<FormatException>(parse);
    }

    [TestCase("pgbin://md/files/0194ad8f61a27f289001111111111111", "md")]
    [TestCase("pgbin://md/files/{0194ad8f-61a2-7f28-9001-111111111111}", "md")]
    [TestCase("pgbin://md/files/0194ad8f-61a2-7f28-9001-11111111111x", "md")]
    public void ParseAddress_RejectsNonCanonicalUuid(string locator, string schema)
    {
        // Arrange
        var repository = new PostgresMessageDataRepository("", schema);
        var address = new Uri(locator, UriKind.RelativeOrAbsolute);

        // Act
        Action parse = () => repository.ParseAddress(address);

        // Assert
        Should.Throw<FormatException>(parse);
    }

    [TestCase("pgbin://m\u00e9d/files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    [TestCase("pgbin://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    [TestCase("pgbin://my-data/files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    public void ParseAddress_RejectsInvalidAuthority(string locator, string schema)
    {
        // Arrange
        var repository = new PostgresMessageDataRepository("", schema);
        var address = new Uri(locator, UriKind.RelativeOrAbsolute);

        // Act
        Action parse = () => repository.ParseAddress(address);

        // Assert
        Should.Throw<FormatException>(parse);
    }

    [TestCase("files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    [TestCase("https://md/files/0194ad8f-61a2-7f28-9001-111111111111", "md")]
    public void ParseAddress_RejectsRelativeOrUnsupportedScheme(string locator, string schema)
    {
        // Arrange
        var repository = new PostgresMessageDataRepository("", schema);
        var address = new Uri(locator, UriKind.RelativeOrAbsolute);

        // Act
        Action parse = () => repository.ParseAddress(address);

        // Assert
        Should.Throw<FormatException>(parse);
    }

    [TestCase("méd")]
    [TestCase("my-schema")]
    [TestCase("has space")]
    [TestCase("quote\"")]
    public void Constructor_RejectsUnsafeSchema(string schema)
    {
        // Arrange
        const string connectionString = "";

        // Act
        var create = () => new PostgresMessageDataRepository(connectionString, schema);

        // Assert
        Should.Throw<ArgumentException>(create);
    }

    [Test]
    public void NormalizeSchema_ConvertsMixedCaseToLowercase()
    {
        // Arrange
        const string schema = "MySchema";

        // Act
        var normalized = PostgresMessageDataRepository.NormalizeSchema(schema);

        // Assert
        normalized.ShouldBe("myschema");
    }

    [Test]
    public void NormalizeSchema_DefaultsEmptySchemaToMd()
    {
        // Arrange
        const string schema = "";

        // Act
        var normalized = PostgresMessageDataRepository.NormalizeSchema(schema);

        // Assert
        normalized.ShouldBe("md");
    }

    [Test]
    public void NormalizeSchema_AcceptsSixtyThreeAsciiCharacters()
    {
        // Arrange
        var schema = new string('A', 63);

        // Act
        var normalized = PostgresMessageDataRepository.NormalizeSchema(schema);

        // Assert
        normalized.ShouldBe(new string('a', 63));
    }

    [Test]
    public void Constructor_RejectsSchemaLongerThanSixtyThreeCharacters()
    {
        // Arrange
        var schema = new string('a', 64);

        // Act
        var create = () => new PostgresMessageDataRepository("", schema);

        // Assert
        Should.Throw<ArgumentException>(create);
    }
}
