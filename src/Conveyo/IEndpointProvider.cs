namespace Conveyo;

public interface IEndpointProvider
{
    IPublishEndpoint GetPublishEndpoint<T>() where T : class;

    ISendEndpoint GetSendEndpoint<T>() where T : class;

    /// <summary>
    /// Returns a send endpoint addressed at the given queue URI (e.g. <c>queue:orders-faults</c>).
    /// </summary>
    ISendEndpoint GetSendEndpoint<T>(Uri address) where T : class;
}
