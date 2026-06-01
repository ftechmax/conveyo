namespace Conveyo;

public interface ISendEndpoint
{
    Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class;
}
