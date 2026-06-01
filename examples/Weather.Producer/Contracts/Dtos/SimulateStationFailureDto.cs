namespace Weather.Producer.Contracts.Dtos;

public record SimulateStationFailureDto
{
    public Guid StationId { get; init; }

    public string Reason { get; init; } = "Swagger-triggered failure";
}
