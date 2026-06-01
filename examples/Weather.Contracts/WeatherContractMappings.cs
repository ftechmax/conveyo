using Conveyo;

namespace Weather.Contracts;

public static class WeatherContractMappings
{
    public static void MapWeatherContracts(this IConveyoBuilder builder)
    {
        builder.Map<SubmitWeatherObservationCommand>("weather:SubmitWeatherObservationCommand.v1");
        builder.Map<UploadWeatherReadingsCommand>("weather:UploadWeatherReadingsCommand.v1");
        builder.Map<UploadRadarImageCommand>("weather:UploadRadarImageCommand.v1");
        builder.Map<UploadSatelliteFeedCommand>("weather:UploadSatelliteFeedCommand.v1");
        builder.Map<SimulateStationFailureCommand>("weather:SimulateStationFailureCommand.v1");
        builder.Map<WeatherObservationRecordedEvent>("weather:WeatherObservationRecordedEvent.v1");
        builder.Map<WeatherSampleArchivedEvent>("weather:WeatherSampleArchivedEvent.v1");
    }
}
