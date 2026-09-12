namespace Conveyo.Storage.Postgres;

internal static class ErrorMessages
{
    public const string MissingConnectionStringConfiguration = "Missing configuration key: postgres:connection-string";
    public const string SchemaInvalidCharacters = "Schema must contain 1 to 63 ASCII letters, digits or underscores.";

    public static string UnsupportedMessageDataUri(Uri address) =>
        $"Unsupported MessageData URI: {address}";

    public static string MessageDataNotFound(Guid id) =>
        $"MessageData not found: {id}";

    public static string InvalidLocator(Uri uri) =>
        $"Invalid locator: {uri}";

    public static string LocatorTargetsDifferentRepository(string schema, string bucket, string configuredSchema) =>
        $"MessageData locator targets pgbin://{schema}/{bucket}; configured repository: pgbin://{configuredSchema}/files.";
}
