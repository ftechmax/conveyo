using Conveyo;
using Weather.Contracts;

namespace Weather.Consumer.Consumers;

public class SimulateStationFailureConsumer : IConsumer<SimulateStationFailureCommand>
{
    public Task Consume(ConsumeContext<SimulateStationFailureCommand> context)
    {
        var msg = context.Message;
        Console.WriteLine(
            $"Received SimulateStationFailureCommand for station {msg.StationId} (failed-at {msg.FailedAt:O}). Throwing to exercise the error routing path.");

        throw new InvalidOperationException(
            $"Intentional station failure requested by producer. Reason: {msg.Reason}");
    }
}
