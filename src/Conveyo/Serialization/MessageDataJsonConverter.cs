using System.Text.Json;
using System.Text.Json.Serialization;

namespace Conveyo.Serialization;

internal sealed class MessageDataJsonConverterFactory : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert)
    {
        return typeToConvert.IsGenericType && typeToConvert.GetGenericTypeDefinition() == typeof(MessageData<>);
    }

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var itemType = typeToConvert.GetGenericArguments()[0];
        var converterType = typeof(MessageDataJsonConverter<>).MakeGenericType(itemType);
        return (JsonConverter)Activator.CreateInstance(converterType)!;
    }
}

internal sealed class MessageDataJsonConverter<T> : JsonConverter<MessageData<T>> where T : class
{
    public override MessageData<T>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return null;
        }

        using var document = JsonDocument.ParseValue(ref reader);
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("MessageData reference must be a JSON object.");
        }

        if (!root.TryGetProperty("address", out var addressElement))
        {
            throw new JsonException("MessageData reference is missing the required 'address' property.");
        }

        if (addressElement.ValueKind != JsonValueKind.String)
        {
            throw new JsonException("MessageData 'address' must be a JSON string.");
        }

        var addressString = addressElement.GetString();
        if (string.IsNullOrWhiteSpace(addressString))
        {
            throw new JsonException("MessageData 'address' must not be empty.");
        }

        if (!Uri.TryCreate(addressString, UriKind.Absolute, out var uri))
        {
            throw new JsonException($"MessageData 'address' is not an absolute URI: '{addressString}'.");
        }

        return new MessageData<T>(uri);
    }

    public override void Write(Utf8JsonWriter writer, MessageData<T> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("address", value.Address.ToString());
        writer.WriteEndObject();
    }
}
