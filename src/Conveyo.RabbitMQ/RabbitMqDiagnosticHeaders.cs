namespace Conveyo.RabbitMQ;

/// <summary>
/// Diagnostic values used by the RabbitMQ transport.
/// </summary>
public static class RabbitMqDiagnosticHeaders
{
    /// <summary>
    /// The OpenTelemetry messaging system value for RabbitMQ spans.
    /// </summary>
    public const string MessagingSystem = "rabbitmq";

    /// <summary>
    /// The diagnostic tag name used for RabbitMQ routing keys.
    /// </summary>
    public const string RoutingKey = "messaging.rabbitmq.destination.routing_key";
}
