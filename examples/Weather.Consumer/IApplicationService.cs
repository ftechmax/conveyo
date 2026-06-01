using Weather.Contracts;

namespace Weather.Consumer;

public interface IApplicationService
{
    Task<WeatherObservationRecordedEvent> RecordAsync(SubmitWeatherObservationCommand command);
}
