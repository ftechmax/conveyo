using Conveyo;

namespace Weather.Contracts;

public record UploadWeatherReadingsCommand
{
    public Guid StationId { get; init; }

    public string Format { get; init; } = "csv";

    public MessageData<string> Readings { get; init; } = null!;
}
