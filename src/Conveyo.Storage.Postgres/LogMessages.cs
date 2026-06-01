namespace Conveyo.Storage.Postgres;

internal static class LogMessages
{
    public const string EnsuringMessageDataSchema = "Ensuring Postgres MessageData schema.";
    public const string MessageDataSchemaReady = "Postgres MessageData schema ready.";
    public const string ExpiredMessageDataCleanupFailed = "Postgres MessageData expiry cleanup sweep failed; will retry on the next interval.";
    public const string ExpiredMessageDataDeleted = "Deleted {Count} expired Postgres MessageData file(s).";
}
