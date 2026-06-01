namespace Conveyo.RabbitMQ;

internal static class ErrorMessages
{
    public const string HostNotConfigured = "RabbitMQ host not configured. Call cfg.Host(...).";
    public const string ChannelNotInitialized = "Channel not initialized.";
    public const string ConnectionNotInitialized = "RabbitMQ connection not initialized.";
    public const string RetryCountCannotBeNegative = "Retry count cannot be negative.";
    public const string EnvelopeByteLimitMustBePositive = "Envelope byte limit must be positive.";
    public const string PortOutOfRange = "Port must be between 1 and 65535.";
    public const string MessageEnvelopeJsonInvalid = "Invalid message envelope JSON.";
    public const string MessageEnvelopeDeserializedToNull = "Message envelope deserialized to null.";
    public const string MissingEnvelopeVersion = "Envelope missing required 'envelopeVersion'.";
    public const string MissingMessageType = "Envelope missing required 'messageType' or contains empty URNs.";
    public const string MissingMessage = "Envelope missing required 'message' or value null.";
    public const string EnvelopeExceededByteLimit = "Message envelope exceeded configured byte limit.";

    public static string NoHandlerFoundForMessageType(Type messageType) =>
        $"No handler found for message type {messageType.FullName}";

    public static string NoQueueFoundForHandlerType(Type handlerType) =>
        $"No queue found for handler type {handlerType.FullName}";

    public static string AmbiguousSendTarget(Type messageType, IEnumerable<string> queues) =>
        $"Message type {messageType.FullName} is consumed on multiple queues ({string.Join(", ", queues)}). " +
        "Send cannot pick one unambiguously; use MapEndpointConvention<T>(address) to choose a default, " +
        "or GetSendEndpoint(address) to target a specific queue.";

    public static string DestinationAddressMustBeQueue(Uri address) =>
        $"Destination address '{address}' must be an absolute queue URI.";

    public static string UnsupportedEnvelopeVersion(string? actualVersion) =>
        $"Unsupported envelope version '{actualVersion}'. Expected '{MessageEnvelope.CurrentEnvelopeVersion}'.";

    public static string MessageEnvelopeBodyExceedsByteLimit(long bodyLength, int maxEnvelopeSizeBytes) =>
        $"Message envelope body is {bodyLength} bytes; exceeds configured {maxEnvelopeSizeBytes} byte limit.";

    public static string SendToQueueUnroutable(string queueName, ushort replyCode, string replyText) =>
        $"Send to queue '{queueName}' failed: unroutable ({replyCode} {replyText}).";

    public static string TerminalQueuePublishFailed(string queueName) =>
        $"Publish to terminal queue '{queueName}' failed.";
}
