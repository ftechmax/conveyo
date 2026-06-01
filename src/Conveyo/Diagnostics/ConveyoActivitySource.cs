using System.Diagnostics;

namespace Conveyo.Diagnostics;

internal static class ConveyoActivitySource
{
    public static readonly ActivitySource Source =
        new(DiagnosticHeaders.DefaultListenerName,
            typeof(ConveyoActivitySource).Assembly.GetName().Version?.ToString());

    public static DistributedContextPropagator Propagator => DistributedContextPropagator.Current;

    public static Activity? StartProducer(
        string messagingSystem,
        string operation,
        string destination,
        MessageEnvelope envelope)
    {
        var spanName = $"{destination} {operation}";
        var activity = Source.StartActivity(spanName, ActivityKind.Producer);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag(DiagnosticHeaders.MessagingSystem, messagingSystem);
        activity.SetTag(DiagnosticHeaders.MessagingOperation, operation);
        activity.SetTag(DiagnosticHeaders.MessagingDestination, destination);
        if (envelope.MessageId is { } id)
        {
            activity.SetTag(DiagnosticHeaders.MessagingMessageId, id.ToString());
        }

        if (envelope.CorrelationId is { } cid)
        {
            activity.SetTag(DiagnosticHeaders.MessagingConversationId, cid.ToString());
        }

        if (envelope.MessageType is { Length: > 0 } types)
        {
            activity.SetTag(DiagnosticHeaders.ConveyoMessageType, types[0]);
        }

        return activity;
    }

    public static Activity? StartConsumer(
        string messagingSystem,
        string destination,
        MessageEnvelope envelope,
        ActivityContext parent)
    {
        var spanName = $"{destination} process";
        var activity = Source.StartActivity(spanName, ActivityKind.Consumer, parent);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag(DiagnosticHeaders.MessagingSystem, messagingSystem);
        activity.SetTag(DiagnosticHeaders.MessagingOperation, "process");
        activity.SetTag(DiagnosticHeaders.MessagingDestination, destination);
        if (envelope.MessageId is { } id)
        {
            activity.SetTag(DiagnosticHeaders.MessagingMessageId, id.ToString());
        }

        if (envelope.CorrelationId is { } cid)
        {
            activity.SetTag(DiagnosticHeaders.MessagingConversationId, cid.ToString());
        }

        if (envelope.MessageType is { Length: > 0 } types)
        {
            activity.SetTag(DiagnosticHeaders.ConveyoMessageType, types[0]);
        }

        return activity;
    }

    public static Activity? StartConsumerError(string messagingSystem, string destination)
    {
        var activity = Source.StartActivity($"{destination} process", ActivityKind.Consumer);
        if (activity is null)
        {
            return null;
        }

        activity.SetTag(DiagnosticHeaders.MessagingSystem, messagingSystem);
        activity.SetTag(DiagnosticHeaders.MessagingOperation, "process");
        activity.SetTag(DiagnosticHeaders.MessagingDestination, destination);
        return activity;
    }
}
