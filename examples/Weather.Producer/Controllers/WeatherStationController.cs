using Weather.Producer.Contracts.Dtos;
using Weather.Producer.Services;
using Microsoft.AspNetCore.Mvc;

namespace Weather.Producer.Controllers;

[ApiController]
[Route("stations")]
public class WeatherStationController(IWeatherService weather) : ControllerBase
{
    [HttpPost("observations")]
    public Task SubmitObservation(SubmitWeatherObservationDto dto)
        => weather.SubmitObservationAsync(dto);

    [HttpPost("samples/readings")]
    public Task UploadReadings(UploadWeatherReadingsDto dto)
        => weather.UploadReadingsAsync(dto);

    [HttpPost("samples/radar")]
    public Task UploadRadarImage([FromForm] UploadRadarImageDto dto)
        => weather.UploadRadarImageAsync(dto);

    [HttpPost("samples/satellite")]
    public Task UploadSatelliteFeed(UploadSatelliteFeedDto dto)
        => weather.UploadSatelliteFeedAsync(dto);

    [HttpPost("failures")]
    public Task SimulateStationFailure(SimulateStationFailureDto dto)
        => weather.SimulateStationFailureAsync(dto);
}
