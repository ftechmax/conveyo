namespace Conveyo;

public interface IMessageDataRepository
{
    Task<Stream> GetAsync(Uri address, CancellationToken cancellationToken = default);

    Task<Uri> PutAsync(Stream data, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default);
}
