using Conveyo;

namespace Weather.Contracts;

public record UploadRadarImageCommand
{
    public Guid StationId { get; init; }

    public string FileName { get; init; } = string.Empty;

    public MessageData<byte[]> Image { get; init; } = null!;
}
