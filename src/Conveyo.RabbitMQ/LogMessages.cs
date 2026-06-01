namespace Conveyo.RabbitMQ;

internal static class LogMessages
{
    public const string BrokerReturnedMessage = "Broker returned message: exchange={Exchange}, routingKey={RoutingKey}, replyCode={ReplyCode}, replyText={ReplyText}";
    public const string EnvelopeTooLarge = "Delivery {DeliveryTag} exceeds envelope size limit; routing metadata to {ErrorQueue}.";
    public const string DeserializationFailed = "Delivery {DeliveryTag} deserialization failed; routing to {ErrorQueue}.";
    public const string Retry = "Retry {Attempt}/{MaxRetries} for message {MessageId} after {Backoff}";
    public const string NoConsumer = "No consumer for message {MessageId}; routing to {SkippedQueue}.";
    public const string MessageHandlingFailed = "Message {MessageId} handling failed ({Attempt}/{TotalAttempts})";
    public const string MessageFailed = "Message {MessageId} failed after {TotalAttempts} attempts; routing to {ErrorQueue}.";
    public const string FaultPublishFailed = "Fault<> publish failed for message {MessageId}; routing to error queue.";
}
