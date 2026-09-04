using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class RabbitMqConnectionManagerTests
{
    private static RabbitMqHostOptions UnreachableBroker() => new()
    {
        ClientName = "conveyo-test",
        Host = "127.0.0.1",
        Port = 1,
        VHost = "/",
        InitialConnectionRetryDelay = TimeSpan.FromMilliseconds(50),
        InitialConnectionMaxRetryDelay = TimeSpan.FromMilliseconds(50),
        InitialConnectionTimeout = TimeSpan.FromMilliseconds(250)
    };

    [Test]
    public async Task StartAsync_RetriesInitialConnection_UntilTimeoutIsSpent()
    {
        var logger = new CapturingLogger();
        var manager = new RabbitMqConnectionManager(logger);
        var options = UnreachableBroker();

        var stopwatch = Stopwatch.StartNew();
        try
        {
            await manager.StartAsync(options, CancellationToken.None);
            Assert.Fail("Expected the initial connection to fail.");
        }
        catch (Exception exception)
        {
            Assert.That(exception, Is.Not.InstanceOf<OperationCanceledException>());
        }

        Assert.Multiple(() =>
        {
            Assert.That(logger.Warnings, Has.Count.GreaterThanOrEqualTo(2), "should have retried more than once");
            Assert.That(logger.Warnings, Has.All.EqualTo(TimeSpan.FromMilliseconds(50)));
            Assert.That(logger.Errors, Has.Count.EqualTo(1), "should log once when it gives up");
            Assert.That(stopwatch.Elapsed, Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(100)));
        });
    }

    [Test]
    public void StartAsync_DoublesRetryDelay_UpToTheCeiling()
    {
        var logger = new CapturingLogger();
        var manager = new RabbitMqConnectionManager(logger);
        var options = UnreachableBroker();
        options.InitialConnectionRetryDelay = TimeSpan.FromMilliseconds(10);
        options.InitialConnectionMaxRetryDelay = TimeSpan.FromMilliseconds(40);
        options.InitialConnectionTimeout = TimeSpan.FromMilliseconds(200);

        Assert.That(async () => await manager.StartAsync(options, CancellationToken.None), Throws.Exception);

        Assert.That(logger.Warnings.Take(4), Is.EqualTo(new[]
        {
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(20),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromMilliseconds(40)
        }));
    }

    [Test]
    public void StartAsync_DoesNotRetry_WhenTimeoutIsZero()
    {
        var logger = new CapturingLogger();
        var manager = new RabbitMqConnectionManager(logger);
        var options = UnreachableBroker();
        options.InitialConnectionTimeout = TimeSpan.Zero;

        Assert.That(async () => await manager.StartAsync(options, CancellationToken.None), Throws.Exception);

        Assert.Multiple(() =>
        {
            Assert.That(logger.Warnings, Is.Empty);
            Assert.That(logger.Errors, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void StartAsync_Throws_WhenCancelledWhileRetrying()
    {
        var manager = new RabbitMqConnectionManager();
        var options = UnreachableBroker();
        options.InitialConnectionTimeout = TimeSpan.FromMinutes(1);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(120));

        Assert.That(
            async () => await manager.StartAsync(options, cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [TestCase(-1, 50, 50, ErrorMessages.InitialConnectionTimeoutCannotBeNegative)]
    [TestCase(250, 0, 50, ErrorMessages.InitialConnectionRetryDelayMustBePositive)]
    [TestCase(250, 50, 10, ErrorMessages.InitialConnectionMaxRetryDelayTooSmall)]
    public void StartAsync_Rejects_InvalidInitialConnectionOptions(
        int timeoutMs, int retryDelayMs, int maxRetryDelayMs, string expectedMessage)
    {
        var manager = new RabbitMqConnectionManager();
        var options = UnreachableBroker();
        options.InitialConnectionTimeout = TimeSpan.FromMilliseconds(timeoutMs);
        options.InitialConnectionRetryDelay = TimeSpan.FromMilliseconds(retryDelayMs);
        options.InitialConnectionMaxRetryDelay = TimeSpan.FromMilliseconds(maxRetryDelayMs);

        Assert.That(
            async () => await manager.StartAsync(options, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>().With.Message.Contains(expectedMessage));
    }

    /// <summary>Collects the RetryDelay of every warning and counts the give-up errors.</summary>
    private sealed class CapturingLogger : ILogger
    {
        public List<TimeSpan> Warnings { get; } = [];

        public List<string> Errors { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (state is not IReadOnlyList<KeyValuePair<string, object?>> values)
            {
                return;
            }

            if (logLevel == LogLevel.Error)
            {
                Errors.Add(formatter(state, exception));
                return;
            }

            foreach (var (key, value) in values)
            {
                if (key == "RetryDelay" && value is TimeSpan delay)
                {
                    Warnings.Add(delay);
                }
            }
        }
    }
}
