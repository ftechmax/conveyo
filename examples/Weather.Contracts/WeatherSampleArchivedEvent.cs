namespace Weather.Contracts;

public record WeatherSampleArchivedEvent
{
    public Guid StationId { get; init; }

    public SampleKind SampleKind { get; init; } = SampleKind.Unknown;

    public string Name { get; init; } = string.Empty;

    public int SizeBytes { get; init; }

    public DateTime ArchivedAt { get; init; } = DateTime.UtcNow;
}
