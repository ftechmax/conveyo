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

    public static readonly TimeSpan DefaultInitialConnectionRetryDelay = TimeSpan.FromSeconds(2);

    public static readonly TimeSpan DefaultInitialConnectionMaxRetryDelay = TimeSpan.FromSeconds(30);

    public static readonly TimeSpan DefaultInitialConnectionTimeout = TimeSpan.FromMinutes(2);

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

    /// <summary>
    /// How long to keep retrying the first connection to the broker before startup fails. Automatic
    /// recovery only covers connections that have been established once, so without this a broker
    /// that is still starting, or credentials an operator has not provisioned yet, aborts host
    /// startup. <c>TimeSpan.Zero</c> fails on the first attempt.
    /// </summary>
    public TimeSpan InitialConnectionTimeout { get; set; } = DefaultInitialConnectionTimeout;

    /// <summary>
    /// Delay before the first retry of the initial connection. Consecutive failures double it up to
    /// <see cref="InitialConnectionMaxRetryDelay"/>.
    /// </summary>
    public TimeSpan InitialConnectionRetryDelay { get; set; } = DefaultInitialConnectionRetryDelay;

    /// <summary>
    /// Upper bound for the exponential backoff applied to repeated initial connection failures. Must
    /// be greater than or equal to <see cref="InitialConnectionRetryDelay"/>.
    /// </summary>
    public TimeSpan InitialConnectionMaxRetryDelay { get; set; } = DefaultInitialConnectionMaxRetryDelay;

    public RabbitMqSslOptions? Ssl { get; set; }
}
