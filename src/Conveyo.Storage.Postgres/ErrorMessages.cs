namespace Conveyo.Storage.Postgres;

internal static class ErrorMessages
{
    public const string MissingConnectionStringConfiguration = "Missing configuration key: postgres:connection-string";
    public const string SchemaInvalidCharacters = "Schema must contain only letters, digits and underscores.";

    public static string UnsupportedUriScheme(Uri address) =>
        $"Unsupported URI scheme: {address.Scheme}";

    public static string MessageDataNotFound(Guid id) =>
        $"MessageData not found: {id}";

    public static string InvalidLocator(Uri uri) =>
        $"Invalid locator: {uri}";

    public static string InvalidLocatorUnsafeSchema(Uri uri) =>
        $"Invalid locator: {uri} (schema must contain only letters, digits and underscores)";

    public static string LocatorTargetsDifferentRepository(string schema, string bucket, string configuredSchema) =>
        $"MessageData locator targets pgbin://{schema}/{bucket}; configured repository: pgbin://{configuredSchema}/files.";
}
