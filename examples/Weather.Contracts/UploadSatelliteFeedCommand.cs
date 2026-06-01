using Conveyo;

namespace Weather.Contracts;

public record UploadSatelliteFeedCommand
{
    public Guid StationId { get; init; }

    public string FeedName { get; init; } = string.Empty;

    public MessageData<Stream> Feed { get; init; } = null!;
}
