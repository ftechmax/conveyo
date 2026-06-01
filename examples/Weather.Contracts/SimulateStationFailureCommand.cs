namespace Weather.Contracts;

public record SimulateStationFailureCommand
{
    public Guid StationId { get; init; }

    public string Reason { get; init; } = "Swagger-triggered failure";

    public DateTime FailedAt { get; init; } = DateTime.UtcNow;
}
