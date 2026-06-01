namespace Weather.Contracts;

public record WeatherObservationRecordedEvent
{
    public Guid ObservationId { get; init; }

    public Guid StationId { get; init; }

    public string Location { get; init; } = string.Empty;

    public double FeelsLikeC { get; init; }

    public string Summary { get; init; } = string.Empty;

    public DateTime RecordedAt { get; init; } = DateTime.UtcNow;
}
