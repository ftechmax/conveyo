namespace Conveyo;

public sealed record HostInfo
{
    public string? MachineName { get; init; }
    public string? ProcessName { get; init; }
    public string? ConveyoVersion { get; init; }
    public string? OperatingSystemVersion { get; init; }
    public string? Runtime { get; init; }
    public string? RuntimeVersion { get; init; }
}
