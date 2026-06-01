using Weather.Producer.Contracts.Dtos;

namespace Weather.Producer.Services;

public interface IWeatherService
{
    Task SubmitObservationAsync(SubmitWeatherObservationDto dto);

    Task UploadReadingsAsync(UploadWeatherReadingsDto dto);

    Task UploadRadarImageAsync(UploadRadarImageDto dto);

    Task UploadSatelliteFeedAsync(UploadSatelliteFeedDto dto);

    Task SimulateStationFailureAsync(SimulateStationFailureDto dto);
}
