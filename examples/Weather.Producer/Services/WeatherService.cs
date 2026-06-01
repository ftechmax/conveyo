using System.Text;
using Conveyo;
using Weather.Contracts;
using Weather.Producer.Contracts.Dtos;

namespace Weather.Producer.Services;

public class WeatherService(IBus bus, IMessageDataRepository? messageDataRepository = null) : IWeatherService
{
    private static readonly TimeSpan SampleTtl = TimeSpan.FromHours(24);

    public Task SubmitObservationAsync(SubmitWeatherObservationDto dto)
    {
        return bus.Send(new SubmitWeatherObservationCommand
        {
            StationId = dto.StationId,
            Location = dto.Location,
            HumidityPercent = dto.HumidityPercent,
            WindSpeedMs = dto.WindSpeedMs,
            PressureHpa = dto.PressureHpa,
            IsPrecipitating = dto.IsPrecipitating,
        });
    }

    public async Task UploadReadingsAsync(UploadWeatherReadingsDto dto)
    {
        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(dto.Readings));
        var readings = new MessageData<string>(await RequireMessageData().PutAsync(ms, SampleTtl));

        await bus.Send(new UploadWeatherReadingsCommand
        {
            StationId = dto.StationId,
            Format = dto.Format,
            Readings = readings,
        });
    }

    public async Task UploadRadarImageAsync(UploadRadarImageDto dto)
    {
        using var buffer = new MemoryStream();
        await dto.Image.CopyToAsync(buffer);
        buffer.Position = 0;
        var image = new MessageData<byte[]>(await RequireMessageData().PutAsync(buffer, SampleTtl));

        await bus.Send(new UploadRadarImageCommand
        {
            StationId = dto.StationId,
            FileName = dto.Image.FileName,
            Image = image,
        });
    }

    public async Task UploadSatelliteFeedAsync(UploadSatelliteFeedDto dto)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(dto.Content));
        var feed = new MessageData<Stream>(await RequireMessageData().PutAsync(stream, SampleTtl));

        await bus.Send(new UploadSatelliteFeedCommand
        {
            StationId = dto.StationId,
            FeedName = dto.FeedName,
            Feed = feed,
        });
    }

    public Task SimulateStationFailureAsync(SimulateStationFailureDto dto)
    {
        return bus.Send(new SimulateStationFailureCommand
        {
            StationId = dto.StationId,
            Reason = dto.Reason,
        });
    }

    private IMessageDataRepository RequireMessageData()
    {
        return messageDataRepository
            ?? throw new InvalidOperationException("No IMessageDataRepository is registered.");
    }
}
