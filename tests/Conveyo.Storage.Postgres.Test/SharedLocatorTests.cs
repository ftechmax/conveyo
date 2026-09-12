using System.Text.Json;

namespace Conveyo.Storage.Postgres.Test;

[TestFixture]
internal class SharedLocatorTests
{
    public static IEnumerable<TestCaseData> Locators() => SharedContracts.Cases("messagedata/postgres-locators.json");

    [TestCaseSource(nameof(Locators))]
    public void LocatorAcceptanceBeforeDatabaseAccess(JsonElement test)
    {
        var repository = new PostgresMessageDataRepository("", test.GetProperty("schema").GetString()!);
        var address = new Uri(test.GetProperty("address").GetString()!, UriKind.RelativeOrAbsolute);
        switch (test.GetProperty("outcome").GetString())
        {
            case "valid":
                Assert.That(repository.ParseAddress(address), Is.EqualTo(Guid.Parse(test.GetProperty("id").GetString()!)));
                break;
            case "namespace":
                Assert.Throws<UnauthorizedAccessException>(() => repository.ParseAddress(address));
                break;
            default:
                Assert.Throws<FormatException>(() => repository.ParseAddress(address));
                break;
        }
    }

    [TestCase("méd")]
    [TestCase("my-schema")]
    [TestCase("has space")]
    [TestCase("quote\"")]
    public void SchemaRejectsNonAsciiOrUnsafeCharacters(string schema) =>
        Assert.Throws<ArgumentException>(() => new PostgresMessageDataRepository("", schema));

    [Test]
    public void SchemaLengthAndNormalization()
    {
        Assert.That(PostgresMessageDataRepository.NormalizeSchema("MySchema"), Is.EqualTo("myschema"));
        Assert.That(PostgresMessageDataRepository.NormalizeSchema(""), Is.EqualTo("md"));
        Assert.That(PostgresMessageDataRepository.NormalizeSchema(new string('A', 63)), Is.EqualTo(new string('a', 63)));
        Assert.Throws<ArgumentException>(() => new PostgresMessageDataRepository("", new string('a', 64)));
    }
}
