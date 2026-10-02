namespace Conveyo.RabbitMQ.Test.Integration;

/// <summary>
/// Stops the connection manager when the test ends, including when an assertion fails.
/// </summary>
internal sealed class ConnectionScope(RabbitMqConnectionManager inner) : IAsyncDisposable
{
    public RabbitMqConnectionManager Inner { get; } = inner;

    public ValueTask DisposeAsync() => new(Inner.StopAsync(CancellationToken.None));
}
