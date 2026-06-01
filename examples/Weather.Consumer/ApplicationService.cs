using Weather.Contracts;

namespace Weather.Consumer;

public class ApplicationService : IApplicationService
{
    private static readonly string[] Summaries =
    [
        "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
    ];

    public Task<WeatherObservationRecordedEvent> RecordAsync(SubmitWeatherObservationCommand command)
    {
        var feelsLike = ComputeFeelsLike(command.PressureHpa, command.WindSpeedMs, command.HumidityPercent);
        var summary = Summaries[Math.Clamp((int)((feelsLike + 20) / 7.5), 0, Summaries.Length - 1)];

        var @event = new WeatherObservationRecordedEvent
        {
            ObservationId = command.ObservationId,
            StationId = command.StationId,
            Location = command.Location,
            FeelsLikeC = feelsLike,
            Summary = command.IsPrecipitating ? $"{summary} & wet" : summary,
        };

        return Task.FromResult(@event);
    }

    private static double ComputeFeelsLike(double pressureHpa, float windSpeedMs, int humidityPercent)
    {
        // Keep the sample deterministic while still reflecting each input field.
        var pressureDelta = (pressureHpa - 1013.25) / 10.0;
        var windChill = windSpeedMs * 0.7;
        var humidityBoost = humidityPercent / 5.0;
        return pressureDelta + humidityBoost - windChill;
    }
}
