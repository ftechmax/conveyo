namespace Weather.Contracts;

public record SubmitWeatherObservationCommand
{
    public Guid ObservationId { get; init; } = Guid.NewGuid();

    public Guid StationId { get; init; }

    public string Location { get; init; } = string.Empty;

    public int HumidityPercent { get; init; }

    public float WindSpeedMs { get; init; }

    public double PressureHpa { get; init; }

    public bool IsPrecipitating { get; init; }

    public DateTime ObservedAt { get; init; } = DateTime.UtcNow;
}
