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
        // Arrange
        var logger = new CapturingLogger();
        var manager = new RabbitMqConnectionManager(logger);
        var options = UnreachableBroker();
        var stopwatch = Stopwatch.StartNew();

        // Act
        var exception = await Should.ThrowAsync<Exception>(() => manager.StartAsync(options, CancellationToken.None));

        // Assert
        (exception is OperationCanceledException).ShouldBeFalse();
        logger.Warnings.Count.ShouldBeGreaterThanOrEqualTo(2, "should have retried more than once");
        logger.Warnings.ShouldAllBe(delay => delay == TimeSpan.FromMilliseconds(50));
        logger.Errors.Count.ShouldBe(1, "should log once when it gives up");
        stopwatch.Elapsed.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(100));
    }

    [Test]
    public async Task StartAsync_DoublesRetryDelay_UpToTheCeiling()
    {
        // Arrange
        var logger = new CapturingLogger();
        var manager = new RabbitMqConnectionManager(logger);
        var options = UnreachableBroker();
        options.InitialConnectionRetryDelay = TimeSpan.FromMilliseconds(10);
        options.InitialConnectionMaxRetryDelay = TimeSpan.FromMilliseconds(40);
        options.InitialConnectionTimeout = TimeSpan.FromMilliseconds(200);

        // Act
        await Should.ThrowAsync<Exception>(() => manager.StartAsync(options, CancellationToken.None));

        // Assert
        logger.Warnings.Take(4).ShouldBe(new[]
        {
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(20),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromMilliseconds(40)
        });
    }

    [Test]
    public async Task StartAsync_DoesNotRetry_WhenTimeoutIsZero()
    {
        // Arrange
        var logger = new CapturingLogger();
        var manager = new RabbitMqConnectionManager(logger);
        var options = UnreachableBroker();
        options.InitialConnectionTimeout = TimeSpan.Zero;

        // Act
        await Should.ThrowAsync<Exception>(() => manager.StartAsync(options, CancellationToken.None));

        // Assert
        logger.Warnings.ShouldBeEmpty();
        logger.Errors.Count.ShouldBe(1);
    }

    [Test]
    public async Task StartAsync_Throws_WhenCancelledWhileRetrying()
    {
        // Arrange
        var manager = new RabbitMqConnectionManager();
        var options = UnreachableBroker();
        options.InitialConnectionTimeout = TimeSpan.FromMinutes(1);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(120));

        // Act
        var start = () => manager.StartAsync(options, cancellation.Token);

        // Assert
        await Should.ThrowAsync<OperationCanceledException>(start);
        manager.Connection.ShouldBeNull();
        manager.ConsumerChannel.ShouldBeNull();
    }

    [Test]
    public async Task StartAsync_RejectsNegativeTimeout()
    {
        // Arrange
        var manager = new RabbitMqConnectionManager();
        var options = UnreachableBroker();
        options.InitialConnectionTimeout = TimeSpan.FromMilliseconds(-1);

        // Act
        var exception = await Should.ThrowAsync<ArgumentOutOfRangeException>(
            () => manager.StartAsync(options, CancellationToken.None));

        // Assert
        exception.Message.ShouldContain(ErrorMessages.InitialConnectionTimeoutCannotBeNegative);
    }

    [Test]
    public async Task StartAsync_RejectsNonPositiveRetryDelay()
    {
        // Arrange
        var manager = new RabbitMqConnectionManager();
        var options = UnreachableBroker();
        options.InitialConnectionRetryDelay = TimeSpan.Zero;

        // Act
        var exception = await Should.ThrowAsync<ArgumentOutOfRangeException>(
            () => manager.StartAsync(options, CancellationToken.None));

        // Assert
        exception.Message.ShouldContain(ErrorMessages.InitialConnectionRetryDelayMustBePositive);
    }

    [Test]
    public async Task StartAsync_RejectsMaximumDelayBelowInitialDelay()
    {
        // Arrange
        var manager = new RabbitMqConnectionManager();
        var options = UnreachableBroker();
        options.InitialConnectionMaxRetryDelay = TimeSpan.FromMilliseconds(10);

        // Act
        var exception = await Should.ThrowAsync<ArgumentOutOfRangeException>(
            () => manager.StartAsync(options, CancellationToken.None));

        // Assert
        exception.Message.ShouldContain(ErrorMessages.InitialConnectionMaxRetryDelayTooSmall);
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
