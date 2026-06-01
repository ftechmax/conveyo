namespace Weather.Producer.Contracts.Dtos;

public record UploadWeatherReadingsDto
{
    public Guid StationId { get; init; }

    public string Format { get; init; } = "csv";

    public string Readings { get; init; } = string.Empty;
}
