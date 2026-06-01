using System.Diagnostics;
using System.Globalization;
using Conveyo.Diagnostics;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqMessageHandler(
    IChannel consumerChannel,
    Func<CancellationToken, Task<IChannel>> publisherChannelFactory,
    ILogger? logger,
    Func<MessageEnvelope, CancellationToken, Task>? onMessageAsync,
    Func<MessageEnvelope, IReadOnlyList<Exception>, CancellationToken, Task>? onFaultAsync = null,
    int maxRetryCount = 3,
    int maxEnvelopeSizeBytes = RabbitMqHostOptions.DefaultMaxEnvelopeSizeBytes,
    bool includeFaultExceptionDetails = false)
{
    internal const string OutcomeHeader = "conveyo-outcome";
    internal const string SkippedReasonHeader = "conveyo-skipped-reason";
    internal const string SkippedOriginalQueueHeader = "conveyo-skipped-original-queue";
    internal const string SkippedOutcomeValue = "skipped";

    internal const string FaultOriginalQueueHeader = "conveyo-fault-original-queue";
    internal const string FaultReasonHeader = "conveyo-fault-reason";
    internal const string FaultExceptionTypeHeader = "conveyo-fault-exception-type";
    internal const string FaultExceptionMessageHeader = "conveyo-fault-exception-message";
    internal const string FaultStackTraceHeader = "conveyo-fault-stack-trace";
    internal const string FaultAttemptsHeader = "conveyo-fault-attempts";
    internal const string FaultTimestampHeader = "conveyo-fault-timestamp";
    internal const string FaultedOutcomeValue = "faulted";
    internal const string FaultReasonException = "exception";
    internal const string FaultReasonDeserializationFailed = "deserialization-failed";
    internal const string FaultReasonEnvelopeTooLarge = "envelope-too-large";

    private readonly int _maxEnvelopeSizeBytes = maxEnvelopeSizeBytes > 0
        ? maxEnvelopeSizeBytes
        : throw new ArgumentOutOfRangeException(nameof(maxEnvelopeSizeBytes), ErrorMessages.EnvelopeByteLimitMustBePositive);

    private readonly int _maxRetryCount = maxRetryCount >= 0
        ? maxRetryCount
        : throw new ArgumentOutOfRangeException(nameof(maxRetryCount), ErrorMessages.RetryCountCannotBeNegative);

    private readonly bool _includeFaultExceptionDetails = includeFaultExceptionDetails;
    private readonly Dictionary<string, Task> _terminalQueueDeclareTasks = new(StringComparer.Ordinal);
    private readonly object _terminalQueueDeclareLock = new();

    public async Task HandleMessageAsync(BasicDeliverEventArgs @event, string queueName)
    {
        // The broker's cancellation token signals consumer cancellation; honour it for IO operations.
        var cancellationToken = @event.CancellationToken;

        if (@event.Body.Length > _maxEnvelopeSizeBytes)
        {
            var ex = new InvalidDataException(
                ErrorMessages.MessageEnvelopeBodyExceedsByteLimit(@event.Body.Length, _maxEnvelopeSizeBytes));

            using var errorActivity = ConveyoActivitySource.StartConsumerError(
                RabbitMqDiagnosticHeaders.MessagingSystem,
                queueName);
            errorActivity?.SetStatus(ActivityStatusCode.Error, ErrorMessages.EnvelopeExceededByteLimit);
            errorActivity?.AddException(ex);

            logger?.LogError(ex,
                LogMessages.EnvelopeTooLarge,
                @event.DeliveryTag, $"{queueName}_error");

            await PublishToErrorQueueAsync(
                ReadOnlyMemory<byte>.Empty,
                queueName,
                FaultReasonEnvelopeTooLarge,
                ex,
                attempts: 1,
                @event.BasicProperties,
                cancellationToken);
            await consumerChannel.BasicAckAsync(@event.DeliveryTag, multiple: false, cancellationToken);
            return;
        }

        MessageEnvelope envelope;
        try
        {
            envelope = EnvelopeSerializer.Deserialize(@event.Body.Span);
        }
        catch (EnvelopeDeserializationException ex)
        {
            using var errorActivity = ConveyoActivitySource.StartConsumerError(
                RabbitMqDiagnosticHeaders.MessagingSystem,
                queueName);
            errorActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            errorActivity?.AddException(ex);

            logger?.LogError(ex,
                LogMessages.DeserializationFailed,
                @event.DeliveryTag, $"{queueName}_error");

            await PublishToErrorQueueAsync(@event.Body, queueName, FaultReasonDeserializationFailed, ex, attempts: 1, @event.BasicProperties, cancellationToken);
            await consumerChannel.BasicAckAsync(@event.DeliveryTag, multiple: false, cancellationToken);
            return;
        }

        if (onMessageAsync == null)
        {
            await consumerChannel.BasicAckAsync(@event.DeliveryTag, multiple: false, cancellationToken);
            return;
        }

        envelope = envelope with { DestinationAddress = QueueAddress.Create(queueName) };

        var parentContext = RabbitMqTraceContextPropagation.Extract(@event.BasicProperties.Headers);
        using var activity = ConveyoActivitySource.StartConsumer(
            RabbitMqDiagnosticHeaders.MessagingSystem,
            queueName,
            envelope,
            parentContext);

        var totalAttempts = _maxRetryCount + 1;
        var capturedExceptions = new List<Exception>(totalAttempts);

        for (var attempt = 1; attempt <= totalAttempts; attempt++)
        {
            try
            {
                if (attempt > 1)
                {
                    // exponential backoff: 1s, 2s, 4s, ...
                    var backoff = TimeSpan.FromSeconds(Math.Pow(2, attempt - 2));
                    activity?.AddEvent(new ActivityEvent("retry", tags: new ActivityTagsCollection
                    {
                        ["attempt"] = attempt - 1
                    }));
                    logger?.LogWarning(
                        LogMessages.Retry,
                        attempt - 1, _maxRetryCount, envelope.MessageId, backoff);
                    await Task.Delay(backoff, cancellationToken);
                }

                await onMessageAsync.Invoke(envelope, cancellationToken);
                await consumerChannel.BasicAckAsync(@event.DeliveryTag, multiple: false, cancellationToken);
                activity?.SetStatus(ActivityStatusCode.Ok);
                return;
            }
            catch (MessageNotConsumedException ex)
            {
                // No consumer registered for this message type so skip retries, route to _skipped queue.
                // Direct-publish (rather than nack) so we can attach a discriminator header explaining why.
                activity?.AddEvent(new ActivityEvent("skipped", tags: new ActivityTagsCollection
                {
                    ["reason"] = ex.Message
                }));
                activity?.SetStatus(ActivityStatusCode.Ok);

                logger?.LogWarning(ex,
                    LogMessages.NoConsumer,
                    envelope.MessageId, $"{queueName}_skipped");

                await PublishToSkippedQueueAsync(@event.Body, queueName, ex.Message, @event.BasicProperties, cancellationToken);
                await consumerChannel.BasicAckAsync(@event.DeliveryTag, multiple: false, cancellationToken);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Shutdown in progress; abandon the delivery without acking so the broker redelivers
                // on reconnect.
                throw;
            }
            catch (Exception ex)
            {
                capturedExceptions.Add(ex);

                if (attempt <= _maxRetryCount)
                {
                    logger?.LogWarning(ex,
                        LogMessages.MessageHandlingFailed,
                        envelope.MessageId, attempt, totalAttempts);
                    continue;
                }

                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddException(ex);

                logger?.LogError(ex,
                    LogMessages.MessageFailed,
                    envelope.MessageId, totalAttempts, $"{queueName}_error");

                await TryPublishFaultAsync(envelope, capturedExceptions, cancellationToken);
                await PublishToErrorQueueAsync(@event.Body, queueName, FaultReasonException, ex, totalAttempts, @event.BasicProperties, cancellationToken);
                await consumerChannel.BasicAckAsync(@event.DeliveryTag, multiple: false, cancellationToken);
                return;
            }
        }
    }

    private async Task TryPublishFaultAsync(MessageEnvelope envelope, IReadOnlyList<Exception> exceptions, CancellationToken cancellationToken)
    {
        if (onFaultAsync is null)
        {
            return;
        }

        try
        {
            await onFaultAsync.Invoke(envelope, exceptions, cancellationToken);
        }
        catch (Exception ex)
        {
            // Fault publication is best-effort, never block error-queue routing on it.
            logger?.LogError(ex,
                LogMessages.FaultPublishFailed,
                envelope.MessageId);
        }
    }

    private async Task PublishToErrorQueueAsync(
        ReadOnlyMemory<byte> body,
        string queueName,
        string reason,
        Exception exception,
        int attempts,
        IReadOnlyBasicProperties? sourceProperties,
        CancellationToken cancellationToken)
    {
        var errorQueue = $"{queueName}_error";

        var headers = new Dictionary<string, object?>
        {
            [OutcomeHeader] = FaultedOutcomeValue,
            [FaultOriginalQueueHeader] = queueName,
            [FaultReasonHeader] = reason,
            [FaultExceptionTypeHeader] = exception.GetType().FullName ?? exception.GetType().Name,
            [FaultExceptionMessageHeader] = _includeFaultExceptionDetails
                ? exception.Message
                : ExceptionInfo.RedactedMessage,
            [FaultAttemptsHeader] = attempts.ToString(CultureInfo.InvariantCulture),
            [FaultTimestampHeader] = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
        };

        // Avoid a null-valued AMQP header when there's no stack trace.
        if (_includeFaultExceptionDetails && exception.StackTrace is not null)
        {
            headers[FaultStackTraceHeader] = exception.StackTrace;
        }

        await PublishTerminalMessageAsync(
            body,
            errorQueue,
            headers,
            sourceProperties,
            cancellationToken);
    }

    private Task PublishToSkippedQueueAsync(
        ReadOnlyMemory<byte> body,
        string queueName,
        string reason,
        IReadOnlyBasicProperties? sourceProperties,
        CancellationToken cancellationToken)
    {
        var skippedQueue = $"{queueName}_skipped";
        var headers = new Dictionary<string, object?>
        {
            [OutcomeHeader] = SkippedOutcomeValue,
            [SkippedReasonHeader] = reason,
            [SkippedOriginalQueueHeader] = queueName,
        };

        return PublishTerminalMessageAsync(
            body,
            skippedQueue,
            headers,
            sourceProperties,
            cancellationToken);
    }

    private async Task PublishTerminalMessageAsync(
        ReadOnlyMemory<byte> body,
        string queue,
        Dictionary<string, object?> headers,
        IReadOnlyBasicProperties? sourceProperties,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var publisherChannel = await publisherChannelFactory(cancellationToken);
            await EnsureTerminalQueueDeclaredAsync(publisherChannel, queue, cancellationToken);

            var properties = RabbitMqMessageProperties.PersistentJson();

            // Carry the original identity/trace metadata forward so the terminal copy stays correlatable.
            if (sourceProperties is not null)
            {
                CopyTerminalProperties(sourceProperties, properties, headers);
            }

            properties.Headers = headers;

            // Publisher confirms ensure the broker has durably accepted the terminal copy before
            // the original delivery is acked.
            await publisherChannel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queue,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            ForgetTerminalQueueDeclaration(queue);
            throw;
        }
        catch (Exception ex)
        {
            ForgetTerminalQueueDeclaration(queue);
            throw new InvalidOperationException(ErrorMessages.TerminalQueuePublishFailed(queue), ex);
        }
    }

    private static void CopyTerminalProperties(
        IReadOnlyBasicProperties source,
        BasicProperties destination,
        Dictionary<string, object?> outcomeHeaders)
    {
        if (source.IsMessageIdPresent())
        {
            destination.MessageId = source.MessageId;
        }

        if (source.IsCorrelationIdPresent())
        {
            destination.CorrelationId = source.CorrelationId;
        }

        if (source.IsTypePresent())
        {
            destination.Type = source.Type;
        }

        if (source.IsTimestampPresent())
        {
            destination.Timestamp = source.Timestamp;
        }

        // Carry original headers forward without clobbering the conveyo-* outcome headers.
        if (source.IsHeadersPresent() && source.Headers is not null)
        {
            foreach (var (key, value) in source.Headers)
            {
                outcomeHeaders.TryAdd(key, value);
            }
        }
    }

    private async Task EnsureTerminalQueueDeclaredAsync(IChannel channel, string queue, CancellationToken cancellationToken)
    {
        Task declareTask;
        lock (_terminalQueueDeclareLock)
        {
            if (!_terminalQueueDeclareTasks.TryGetValue(queue, out declareTask!))
            {
                declareTask = RabbitMqTopology.DeclareDurableQueueAsync(channel, queue, cancellationToken);
                _terminalQueueDeclareTasks[queue] = declareTask;
            }
        }

        try
        {
            await declareTask;
        }
        catch
        {
            lock (_terminalQueueDeclareLock)
            {
                if (_terminalQueueDeclareTasks.TryGetValue(queue, out var cached) && ReferenceEquals(cached, declareTask))
                {
                    _terminalQueueDeclareTasks.Remove(queue);
                }
            }

            throw;
        }
    }

    private void ForgetTerminalQueueDeclaration(string queue)
    {
        lock (_terminalQueueDeclareLock)
        {
            _terminalQueueDeclareTasks.Remove(queue);
        }
    }

}
