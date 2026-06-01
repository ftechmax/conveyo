namespace Conveyo;

internal static class OutboundContext
{
    private static readonly AsyncLocal<OutboundMetadata?> _current = new();

    internal static OutboundMetadata? Current => _current.Value;

    internal static IDisposable Push(OutboundMetadata metadata)
    {
        var previous = _current.Value;
        _current.Value = metadata;
        return new PopOnDispose(previous);
    }

    private sealed class PopOnDispose(OutboundMetadata? previous) : IDisposable
    {
        public void Dispose() => _current.Value = previous;
    }
}

internal sealed record OutboundMetadata(
    Guid? CorrelationId,
    IReadOnlyDictionary<string, string>? Headers);
