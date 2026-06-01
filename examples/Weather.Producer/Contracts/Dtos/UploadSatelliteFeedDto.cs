namespace Weather.Producer.Contracts.Dtos;

public record UploadSatelliteFeedDto
{
    public Guid StationId { get; init; }

    public string FeedName { get; init; } = "satellite.feed";

    public string Content { get; init; } = string.Empty;
}
