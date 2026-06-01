namespace Conveyo;

public interface ConsumeContext<out T> :
    IPublishEndpoint,
    ISendEndpoint
    where T : class
{
    T Message { get; }

    /// <summary>The unique identifier of this message, as set by the publisher.</summary>
    Guid? MessageId { get; }

    /// <summary>
    /// Correlation identifier propagated by the publisher.
    /// </summary>
    Guid? CorrelationId { get; }

    /// <summary>The queue address this delivery arrived on.</summary>
    Uri? DestinationAddress { get; }

    /// <summary>The publisher-supplied UTC timestamp.</summary>
    DateTime? SentTime { get; }

    /// <summary>Information about the host that published the message.</summary>
    HostInfo? Host { get; }

    /// <summary>
    /// Application-level headers attached to the inbound message. Headers are propagated to any
    /// outgoing publish/send issued through this context.
    /// </summary>
    IReadOnlyDictionary<string, string>? Headers { get; }

    /// <summary>
    /// The cancellation token bound to this delivery's lifecycle. Honour this in long-running
    /// handlers so the host can shut down promptly.
    /// </summary>
    CancellationToken CancellationToken { get; }
}

internal sealed class ConsumeContextImpl<T>(MessageEnvelope envelope, T message, IEndpointProvider endpointProvider, CancellationToken cancellationToken) : ConsumeContext<T> where T : class
{
    public Guid? MessageId { get; } = envelope.MessageId;

    public Guid? CorrelationId { get; } = envelope.CorrelationId;

    public Uri? DestinationAddress { get; } = envelope.DestinationAddress;

    public DateTime? SentTime { get; } = envelope.SentTime;

    public HostInfo? Host { get; } = envelope.Host;

    public IReadOnlyDictionary<string, string>? Headers { get; } = envelope.Headers;

    public CancellationToken CancellationToken { get; } = cancellationToken;

    public T Message { get; } = message;

    public Task Publish<TMessage>(TMessage message, CancellationToken cancellationToken = default) where TMessage : class
    {
        var endpoint = endpointProvider.GetPublishEndpoint<TMessage>();
        // The AsyncLocal correlation push happens at the ConveyoHostedService boundary so it stays
        // live across the whole consumer scope; endpoints just read OutboundContext.Current.
        return endpoint.Publish(message, cancellationToken);
    }

    public Task Send<TMessage>(TMessage message, CancellationToken cancellationToken = default) where TMessage : class
    {
        var endpoint = endpointProvider.GetSendEndpoint<TMessage>();
        return endpoint.Send(message, cancellationToken);
    }
}
