using Conveyo;
using Weather.Contracts;

namespace Weather.Producer.Consumers;

public class LocalEventHandler : IConsumer<WeatherObservationRecordedEvent>, IConsumer<WeatherSampleArchivedEvent>
{
    public Task Consume(ConsumeContext<WeatherObservationRecordedEvent> context)
    {
        var msg = context.Message;
        Console.WriteLine(
            $"[observation recorded] station={msg.StationId} location={msg.Location} feels-like={msg.FeelsLikeC:F1}°C summary='{msg.Summary}' at={msg.RecordedAt:O}");
        return Task.CompletedTask;
    }

    public Task Consume(ConsumeContext<WeatherSampleArchivedEvent> context)
    {
        var msg = context.Message;
        Console.WriteLine(
            $"[sample archived] station={msg.StationId} kind={msg.SampleKind} name='{msg.Name}' size={msg.SizeBytes}B at={msg.ArchivedAt:O}");
        return Task.CompletedTask;
    }
}
