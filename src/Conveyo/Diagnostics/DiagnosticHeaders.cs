namespace Conveyo.Diagnostics;

public static class DiagnosticHeaders
{
    public const string DefaultListenerName = "Conveyo";

    public const string TraceParent = "traceparent";
    public const string TraceState = "tracestate";

    public const string MessagingSystem = "messaging.system";
    public const string MessagingOperation = "messaging.operation.type";
    public const string MessagingDestination = "messaging.destination.name";
    public const string MessagingMessageId = "messaging.message.id";
    public const string MessagingConversationId = "messaging.message.conversation_id";
    public const string MessagingBodySize = "messaging.message.body.size";
    public const string ConveyoMessageType = "conveyo.message_type";
}
