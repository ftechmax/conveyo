namespace Conveyo;

internal static class LogMessages
{
    public const string Starting = "Conveyo starting.";
    public const string Stopping = "Conveyo stopping.";
    public const string CannotPublishFaultNoMessageType = "Cannot publish Fault<>: missing MessageType.";
    public const string CannotPublishFaultNoDispatchInfo = "Cannot publish Fault<> for {MessageId}: no dispatch info for [{MessageTypes}].";
    public const string ProcessingMessageDataProperty = "Processing MessageData property {PropertyName}";
    public const string FetchingMessageData = "Fetching MessageData from {Address}";
    public const string SkippingMessageDataNullPayload = "Skipping MessageData assignment for {PropertyName}: payload null.";
    public const string SkippingMessageDataReadOnly = "Skipping MessageData assignment for {PropertyName}: property read-only.";
    public const string SettingMessageDataProperty = "Setting MessageData property {PropertyName}";
    public const string ResolvingConsumer = "Resolving consumer {HandlerType}";
    public const string MessageHandlingFailed = "Message handling failed.";
}
