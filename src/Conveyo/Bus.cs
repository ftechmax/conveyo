namespace Conveyo;

public interface IBus : IPublishEndpoint, ISendEndpoint
{
}

internal sealed class Bus(IEndpointProvider endpointProvider) : IBus
{
    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        var endpoint = endpointProvider.GetPublishEndpoint<T>();
        return endpoint.Publish(message, cancellationToken);
    }

    public Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        var endpoint = endpointProvider.GetSendEndpoint<T>();
        return endpoint.Send(message, cancellationToken);
    }
}
