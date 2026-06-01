using System.Text;
using Conveyo;
using Weather.Contracts;

namespace Weather.Consumer.Consumers;

public class WeatherSampleConsumer
    : IConsumer<UploadWeatherReadingsCommand>,
      IConsumer<UploadRadarImageCommand>,
      IConsumer<UploadSatelliteFeedCommand>
{
    public async Task Consume(ConsumeContext<UploadWeatherReadingsCommand> context)
    {
        var msg = context.Message;
        Console.WriteLine($"Received UploadWeatherReadingsCommand for station {msg.StationId} ({msg.Format})");

        var sizeBytes = 0;
        if (msg.Readings?.Value is { } text)
        {
            sizeBytes = Encoding.UTF8.GetByteCount(text);
            Console.WriteLine($"Readings ({text.Length} chars / {sizeBytes} bytes): {Preview(text)}");
        }
        else
        {
            Console.WriteLine("No readings payload provided");
        }

        await PublishArchived(context, msg.StationId, SampleKind.Readings, msg.Format, sizeBytes);
    }

    public async Task Consume(ConsumeContext<UploadRadarImageCommand> context)
    {
        var msg = context.Message;
        Console.WriteLine($"Received UploadRadarImageCommand for station {msg.StationId} ({msg.FileName})");

        var sizeBytes = 0;
        if (msg.Image?.Value is { } bytes)
        {
            sizeBytes = bytes.Length;
            Console.WriteLine($"Image bytes received: {sizeBytes} bytes");
        }
        else
        {
            Console.WriteLine("No image payload provided");
        }

        await PublishArchived(context, msg.StationId, SampleKind.Radar, msg.FileName, sizeBytes);
    }

    public async Task Consume(ConsumeContext<UploadSatelliteFeedCommand> context)
    {
        var msg = context.Message;
        Console.WriteLine($"Received UploadSatelliteFeedCommand for station {msg.StationId} ({msg.FeedName})");

        var sizeBytes = 0;
        if (msg.Feed?.Value is { } stream)
        {
            await using (stream)
            {
                if (stream.CanSeek)
                {
                    stream.Position = 0;
                }

                using var buffer = new MemoryStream();
                await stream.CopyToAsync(buffer);
                sizeBytes = (int)buffer.Length;
                var preview = Preview(Encoding.UTF8.GetString(buffer.ToArray()));
                Console.WriteLine($"Feed stream received: {sizeBytes} bytes — preview: {preview}");
            }
        }
        else
        {
            Console.WriteLine("No feed payload provided");
        }

        await PublishArchived(context, msg.StationId, SampleKind.Satellite, msg.FeedName, sizeBytes);
    }

    private static Task PublishArchived<T>(ConsumeContext<T> context, Guid stationId, SampleKind kind, string name, int sizeBytes)
        where T : class
    {
        var @event = new WeatherSampleArchivedEvent
        {
            StationId = stationId,
            SampleKind = kind,
            Name = name,
            SizeBytes = sizeBytes,
        };

        Console.WriteLine($"Publishing WeatherSampleArchivedEvent ({kind}, {sizeBytes}B)");
        return context.Publish(@event);
    }

    private static string Preview(string text)
        => text.Length <= 80 ? text : text[..80] + "…";
}
