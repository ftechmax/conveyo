namespace Conveyo.RabbitMQ;

/// <summary>
/// Thrown when a <see cref="ISendEndpoint.Send{T}"/> publish is returned by the broker because no consumer
/// queue is bound to receive it. This implements the documented "unroutable = failure" command semantic:
/// a Send completes only after the broker has confirmed the message was routed to at least one queue.
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
