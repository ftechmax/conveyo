namespace Weather.Producer.Contracts.Dtos;

public record UploadRadarImageDto
{
    public Guid StationId { get; init; }

    public required IFormFile Image { get; init; }
}
