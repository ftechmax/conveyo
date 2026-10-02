namespace Conveyo.RabbitMQ;

/// <summary>
/// Thrown when the broker returns a mandatory <see cref="ISendEndpoint.Send{T}"/>
/// publish as unroutable.
/// </summary>
public sealed class UnroutableMessageException : Exception
{
    public UnroutableMessageException(string message) : base(message)
    {
    }

    public UnroutableMessageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public string? Exchange { get; init; }

    public string? RoutingKey { get; init; }

    public ushort? ReplyCode { get; init; }

    public string? ReplyText { get; init; }
}
