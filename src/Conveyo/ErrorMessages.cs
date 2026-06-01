namespace Conveyo;

internal static class ErrorMessages
{
    public const string MessageDeserializationFailed = "Message deserialization failed.";
    public const string MessageDataByteLimitMustBePositive = "MessageData byte limit must be positive.";
    public const string ByteLimitMustBeNonNegative = "Byte limit must be non-negative.";
    public const string MalformedDataUriMissingColon = "Malformed data URI: missing ':'.";
    public const string MalformedDataUriMissingComma = "Malformed data URI: missing ','.";
    public const string MalformedDataUriRequiresBase64 = "Malformed data URI: base64 payload required.";
    public const string UrnRequired = "URN required.";

    public static string NoTypeRegisteredForUrns(IEnumerable<string?> messageTypes) =>
        $"No registered message type for URNs [{string.Join(", ", messageTypes)}].";

    public static string NoConsumerRegisteredForMessageType(Type type) =>
        $"No consumer for message type {type.FullName}.";

    public static string NoHandlerFoundForMessageType(Type type) =>
        $"No handler found for message type {type.FullName}";

    public static string NoHandlerFoundForMessageTypeAtDestination(Type type, Uri destinationAddress) =>
        $"No handler found for message type {type.FullName} at destination {destinationAddress}";

    public static string ServiceTypeNotFound(Type handlerType) =>
        $"Service {handlerType.FullName} not found.";

    public static string CannotHydrateMessageDataWithoutRepository(Uri address) =>
        $"Cannot hydrate MessageData from {address}: IMessageDataRepository not registered.";

    public static string DestinationAddressMustBeQueue(Uri address) =>
        $"Destination address must use the queue: scheme. Received {address}.";

    public static string MessageDataPayloadExceedsByteLimit(Uri address, long maxBytes) =>
        $"MessageData payload at {address} exceeds the configured {maxBytes} byte limit.";

    public static string ExpectedDataUri(Uri uri) =>
        $"Expected data URI, got '{uri.Scheme}:'.";

    public static string DataUriPayloadExceedsByteLimit(long maxBytes) =>
        $"Data URI payload exceeds the configured {maxBytes} byte limit.";

    public static string MessageTypeHasNoUrnMapping(Type type) =>
        $"{type.FullName} has no URN mapping. Call cfg.Map<{type.Name}>(\"urn:...\").";

    public static string UrnAlreadyRegistered(string urn, Type existingType, Type type) =>
        $"URN '{urn}' already maps to {existingType.FullName}; cannot map to {type.FullName}.";

    public static string UrnExceedsAmqpLimit(string urn) =>
        $"URN '{urn}' exceeds AMQP's 255-octet limit.";

    public static string UrnContainsInvalidCharacters(string urn) =>
        $"URN '{urn}' contains invalid characters. Allowed: letters, digits, '.', '-', '_', ':'.";

    public static string ConsumedMessageTypesHaveNoUrnMapping(IEnumerable<Type> unmapped) =>
        "Consumed message types missing URN mappings: " +
        string.Join(", ", unmapped.Select(t => t.FullName)) +
        ". Call cfg.Map<T>(\"urn:...\").";
}
