namespace Weather.Producer.Contracts.Dtos;

public record SubmitWeatherObservationDto
{
    public Guid StationId { get; init; }

    public string Location { get; init; } = string.Empty;

    public int HumidityPercent { get; init; }

    public float WindSpeedMs { get; init; }

    public double PressureHpa { get; init; }

    public bool IsPrecipitating { get; init; }
}
