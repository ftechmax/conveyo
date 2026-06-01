using Conveyo;
using Weather.Contracts;

namespace Weather.Consumer.Consumers;

public class SubmitWeatherObservationConsumer(IApplicationService applicationService)
    : IConsumer<SubmitWeatherObservationCommand>
{
    public async Task Consume(ConsumeContext<SubmitWeatherObservationCommand> context)
    {
        var msg = context.Message;
        Console.WriteLine(
            $"Received SubmitWeatherObservationCommand: station={msg.StationId} location='{msg.Location}' " +
            $"humidity={msg.HumidityPercent}% wind={msg.WindSpeedMs}m/s pressure={msg.PressureHpa}hPa " +
            $"precip={msg.IsPrecipitating} observed={msg.ObservedAt:O}");

        var @event = await applicationService.RecordAsync(msg);

        Console.WriteLine($"Publishing WeatherObservationRecordedEvent (feels-like {@event.FeelsLikeC:F1}°C, '{@event.Summary}')");
        await context.Publish(@event);
    }
}
