namespace Conveyo;

public sealed record Fault<T> where T : class
{
    public required Guid FaultId { get; init; }
    public required Guid? FaultedMessageId { get; init; }
    public required DateTime Timestamp { get; init; }
    public required ExceptionInfo[] Exceptions { get; init; }
    public required HostInfo Host { get; init; }
    public required T Message { get; init; }
}

public sealed record ExceptionInfo
{
    public const string RedactedMessage = "Exception details redacted.";

    public required string ExceptionType { get; init; }
    public required string Message { get; init; }
    public string? StackTrace { get; init; }
    public ExceptionInfo? InnerException { get; init; }

    public static ExceptionInfo From(Exception exception, bool includeDetails = false)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return new ExceptionInfo
        {
            ExceptionType = exception.GetType().FullName ?? exception.GetType().Name,
            Message = includeDetails ? exception.Message : RedactedMessage,
            StackTrace = includeDetails ? exception.StackTrace : null,
            InnerException = includeDetails && exception.InnerException is { } inner
                ? From(inner, includeDetails)
                : null
        };
    }
}
