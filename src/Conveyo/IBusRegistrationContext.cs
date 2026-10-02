namespace Conveyo;

internal interface IBusRegistrationContext
{
    Task StartAsync(ConveyoContext context, CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);

    event Func<MessageEnvelope, CancellationToken, Task>? OnMessageAsync;

    /// <summary>
    /// Invokes fault publication after dispatch retries are exhausted and before error-queue routing.
    /// Fault publication failures do not prevent the transport from routing the original delivery.
    /// </summary>
    event Func<MessageEnvelope, IReadOnlyList<Exception>, CancellationToken, Task>? OnFaultAsync;
}
