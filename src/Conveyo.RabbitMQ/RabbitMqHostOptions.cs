namespace Conveyo.RabbitMQ;

public sealed record RabbitMqHostOptions
{
    public const int DefaultPort = 5672;

    public const int TlsPort = 5671;

    public const int DefaultMaxEnvelopeSizeBytes = 1024 * 1024;

    /// <summary>
    /// Default per-consumer prefetch. Bounds the number of unacknowledged in-flight deliveries the broker
    /// will dispatch to this channel; this is the consumer-side backpressure knob.
    /// </summary>
    public const ushort DefaultPrefetchCount = 16;

    public required string ClientName { get; init; }

    public required string Host { get; init; }

    public required int Port { get; set; }

    public required string VHost { get; init; }

    public string Username { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public int MaxRetryCount { get; set; } = 3;

    public int MaxEnvelopeSizeBytes { get; set; } = DefaultMaxEnvelopeSizeBytes;

    public bool IncludeFaultExceptionDetails { get; set; }

    /// <summary>
    /// Per-channel prefetch count. <c>0</c> means unlimited.
    /// </summary>
    public ushort PrefetchCount { get; set; } = DefaultPrefetchCount;

    /// <summary>
    /// Deliveries the shared consumer channel dispatches concurrently. At <c>1</c> (the default),
    /// dispatch is sequential per channel, so a handler in retry backoff blocks all other queues.
    /// Raising it lets other deliveries proceed while one is delayed, at the cost of delivery ordering.
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; set; } = 1;

    /// <summary>
    /// Delay between automatic reconnection attempts after the connection drops. Topology and consumers are re-declared automatically on reconnect.
    /// </summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// AMQP heartbeat. Lower values detect dead connections faster; higher values reduce noise on
    /// long-idle connections. <c>0</c> disables heartbeats.
    /// </summary>
    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(60);

    public RabbitMqSslOptions? Ssl { get; set; }
}
