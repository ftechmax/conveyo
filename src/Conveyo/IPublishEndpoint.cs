namespace Conveyo;

public interface IPublishEndpoint
{
    Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class;
}
