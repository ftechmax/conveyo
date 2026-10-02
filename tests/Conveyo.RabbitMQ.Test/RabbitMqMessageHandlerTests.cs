using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public partial class RabbitMqMessageHandlerTests
{
    [Test]
    public void Constructor_RejectsNegativeRetryCount()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();

        // Act
        var ex = Should.Throw<ArgumentOutOfRangeException>(() => _ = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => Task.CompletedTask,
            maxRetryCount: -1));

        // Assert
        ex.ParamName.ShouldBe("maxRetryCount");
    }

    [Test]
    public async Task HandleMessageAsync_MalformedEnvelopePublishesToErrorQueueAndAcks()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new AssertionException("Malformed envelopes should not reach consumers."));

        // Act
        await handler.HandleMessageAsync(CreateDelivery("{not-json"), "orders");

        // Assert
        publisher.PublishedRoutingKeys.ShouldBe(new[] { "orders_error" });
        publisher.PublishedMandatoryFlags.ShouldBe(new[] { true });
        var properties = publisher.PublishedProperties.Single();
        properties.Headers.ShouldNotBeNull();
        properties.Headers!["conveyo-outcome"].ShouldBe("faulted");
        properties.Headers["conveyo-fault-reason"].ShouldBe("deserialization-failed");
        properties.Headers["conveyo-fault-original-queue"].ShouldBe("orders");
        properties.Headers["conveyo-fault-attempts"].ShouldBe("1");
        properties.Headers["conveyo-fault-exception-type"].ShouldNotBeNull();
        properties.Headers["conveyo-fault-exception-message"].ShouldBe(ExceptionInfo.RedactedMessage);
        properties.Headers.ContainsKey("conveyo-fault-stack-trace").ShouldBeFalse();
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
    }

    [Test]
    public async Task HandleMessageAsync_OversizedEnvelopePublishesRedactedMetadataToErrorQueueAndAcks()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new AssertionException("Oversized envelopes should not reach consumers."),
            maxEnvelopeSizeBytes: 4);

        // Act
        await handler.HandleMessageAsync(CreateDelivery("12345"), "orders");

        // Assert
        publisher.PublishedRoutingKeys.ShouldBe(new[] { "orders_error" });
        var properties = publisher.PublishedProperties.Single();
        properties.Headers.ShouldNotBeNull();
        properties.Headers!["conveyo-fault-reason"].ShouldBe("envelope-too-large");
        properties.Headers["conveyo-fault-exception-message"].ShouldBe(ExceptionInfo.RedactedMessage);
        properties.Headers.ContainsKey("conveyo-fault-stack-trace").ShouldBeFalse();
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
    }

    [Test]
    public async Task HandleMessageAsync_ConsumerExceptionAfterRetriesPublishesToErrorQueueAndAcks()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            maxRetryCount: 0);

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders");

        // Assert
        publisher.DeclaredQueues.ShouldBe(new[] { "orders_error" });
        publisher.Operations.ShouldBe(new[] { "declare:orders_error", "publish:orders_error" });
        publisher.PublishedRoutingKeys.ShouldBe(new[] { "orders_error" });
        publisher.PublishedMandatoryFlags.ShouldBe(new[] { true });
        var properties = publisher.PublishedProperties.Single();
        properties.Headers.ShouldNotBeNull();
        properties.Headers!["conveyo-outcome"].ShouldBe("faulted");
        properties.Headers["conveyo-fault-reason"].ShouldBe("exception");
        properties.Headers["conveyo-fault-original-queue"].ShouldBe("orders");
        properties.Headers["conveyo-fault-exception-type"].ShouldBe("System.InvalidOperationException");
        properties.Headers["conveyo-fault-exception-message"].ShouldBe(ExceptionInfo.RedactedMessage);
        properties.Headers.ContainsKey("conveyo-fault-stack-trace").ShouldBeFalse();
        properties.Headers["conveyo-fault-attempts"].ShouldBe("1");
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
    }

    [Test]
    public async Task HandleMessageAsync_IncludesExceptionDetailsInErrorHeadersWhenConfigured()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            maxRetryCount: 0,
            includeFaultExceptionDetails: true);

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders");

        // Assert
        var properties = publisher.PublishedProperties.Single();
        properties.Headers.ShouldNotBeNull();
        properties.Headers!["conveyo-fault-exception-type"].ShouldBe("System.InvalidOperationException");
        properties.Headers["conveyo-fault-exception-message"].ShouldBe("consumer failed");
        properties.Headers.ContainsKey("conveyo-fault-stack-trace").ShouldBeTrue();
    }

    [Test]
    public async Task HandleMessageAsync_InvokesFaultHookWithAccumulatedExceptionsBeforePublishingToErrorQueue()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var attempt = 0;
        MessageEnvelope? observedEnvelope = null;
        IReadOnlyList<Exception>? observedExceptions = null;
        string[]? publishedBeforeFault = null;
        ulong[]? acknowledgedBeforeFault = null;

        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) =>
            {
                attempt++;
                throw new InvalidOperationException($"failure #{attempt}");
            },
            onFaultAsync: (envelope, exceptions, _) =>
            {
                observedEnvelope = envelope;
                observedExceptions = exceptions.ToArray();
                publishedBeforeFault = publisher.PublishedRoutingKeys.ToArray();
                acknowledgedBeforeFault = consumer.AckedDeliveryTags.ToArray();
                return Task.CompletedTask;
            },
            maxRetryCount: 0);

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders");

        // Assert
        observedEnvelope.ShouldNotBeNull();
        observedExceptions.ShouldNotBeNull();
        observedExceptions.Select(ex => ex.Message).ShouldBe(new[] { "failure #1" });
        publishedBeforeFault.ShouldNotBeNull();
        publishedBeforeFault.ShouldBeEmpty("fault hook must run before publishing to the error queue");
        acknowledgedBeforeFault.ShouldNotBeNull();
        acknowledgedBeforeFault.ShouldBeEmpty("fault hook must run before ack");
        publisher.PublishedRoutingKeys.ShouldBe(new[] { "orders_error" });
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
    }

    [Test]
    public async Task HandleMessageAsync_FaultHookExceptionDoesNotBlockErrorPublishOrAck()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            onFaultAsync: (_, _, _) => throw new InvalidOperationException("fault publish failed"),
            maxRetryCount: 0);

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders");

        // Assert
        publisher.PublishedRoutingKeys.ShouldBe(new[] { "orders_error" });
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
    }

    [Test]
    public async Task HandleMessageAsync_FaultHookNotInvokedForSkippedMessages()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var faultInvoked = false;
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new MessageNotConsumedException("no consumer for type"),
            onFaultAsync: (_, _, _) =>
            {
                faultInvoked = true;
                return Task.CompletedTask;
            });

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("hello")), "orders");

        // Assert
        faultInvoked.ShouldBeFalse();
    }

    [Test]
    public async Task HandleMessageAsync_DoesNotAckWhenErrorPublishFails()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        publisher.PublishException = new InvalidOperationException("error publish failed");
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            maxRetryCount: 0);

        // Act
        var ex = await Should.ThrowAsync<InvalidOperationException>(
            () => handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders"));

        // Assert
        ex.Message.ShouldBe(ErrorMessages.TerminalQueuePublishFailed("orders_error"));
        ex.InnerException.ShouldNotBeNull();
        ex.InnerException.GetType().ShouldBe(typeof(InvalidOperationException));
        ex.InnerException!.Message.ShouldBe("error publish failed");
        consumer.AckedDeliveryTags.ShouldBeEmpty();
    }

    [Test]
    public async Task HandleMessageAsync_SkippedMessagePublishesToSkippedQueueWithDiscriminatorHeader()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new MessageNotConsumedException("no consumer for type"));

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("hello")), "orders");

        // Assert
        publisher.DeclaredQueues.ShouldBe(new[] { "orders_skipped" });
        publisher.Operations.ShouldBe(new[] { "declare:orders_skipped", "publish:orders_skipped" });
        publisher.PublishedRoutingKeys.ShouldBe(new[] { "orders_skipped" });
        publisher.PublishedMandatoryFlags.ShouldBe(new[] { true });
        var properties = publisher.PublishedProperties.Single();
        properties.Persistent.ShouldBeTrue();
        properties.Headers.ShouldNotBeNull();
        properties.Headers!["conveyo-outcome"].ShouldBe("skipped");
        properties.Headers["conveyo-skipped-reason"].ShouldBe("no consumer for type");
        properties.Headers["conveyo-skipped-original-queue"].ShouldBe("orders");
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
    }

    [Test]
    public async Task HandleMessageAsync_DeclaresTerminalQueueBeforeEachPublish()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            maxRetryCount: 0);

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("first")), "orders");
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("second")), "orders");

        // Assert
        publisher.DeclaredQueues.ShouldBe(new[] { "orders_error", "orders_error" });
        publisher.PublishedRoutingKeys.ShouldBe(new[] { "orders_error", "orders_error" });
    }

    private static BasicDeliverEventArgs CreateDelivery(string body)
    {
        return new BasicDeliverEventArgs(
            consumerTag: "consumer",
            deliveryTag: 42,
            redelivered: false,
            exchange: string.Empty,
            routingKey: "orders",
            properties: new BasicProperties(),
            body: Encoding.UTF8.GetBytes(body),
            cancellationToken: CancellationToken.None);
    }

    private static BasicDeliverEventArgs CreateDelivery<T>(T message)
        where T : class
    {
        var body = EnvelopeSerializer.Serialize(EnvelopeSerializer.Create(message, new HostInfo(), "conveyo:test.example.v1"));
        return new BasicDeliverEventArgs(
            consumerTag: "consumer",
            deliveryTag: 42,
            redelivered: false,
            exchange: string.Empty,
            routingKey: "orders",
            properties: new BasicProperties(),
            body: body,
            cancellationToken: CancellationToken.None);
    }

    private sealed record ExampleMessage(string Value);

}
