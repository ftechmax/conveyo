namespace Conveyo;

internal interface IBusRegistrationContext
{
    Task StartAsync(ConveyoContext context, CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);

    event Func<MessageEnvelope, CancellationToken, Task>? OnMessageAsync;

    /// <summary>
    /// Raised after the transport has exhausted retries on a delivery. Subscribers are expected to
    /// publish <see cref="Fault{T}"/> for the failed message; the transport then routes the original
    /// delivery to the error queue once this completes.
    /// </summary>
    event Func<MessageEnvelope, IReadOnlyList<Exception>, CancellationToken, Task>? OnFaultAsync;
}
