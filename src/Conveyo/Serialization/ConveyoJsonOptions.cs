using System.Text.Json;

namespace Conveyo.Serialization;

internal static class ConveyoJsonOptions
{
    public static JsonSerializerOptions Default { get; } = Build();

    private static JsonSerializerOptions Build()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            WriteIndented = false,
        };
        options.Converters.Add(new MessageDataJsonConverterFactory());
        return options;
    }
}
