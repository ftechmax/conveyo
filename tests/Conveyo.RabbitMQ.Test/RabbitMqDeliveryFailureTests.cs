namespace Conveyo.RabbitMQ.Test;

public partial class RabbitMqMessageHandlerTests
{
    [Test]
    public async Task HandleMessageAsync_SuccessAcknowledgesWithoutTerminalPublication()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var calls = 0;
        var handler = new RabbitMqMessageHandler(consumer.Channel,
            _ => throw new InvalidOperationException("Success must not open a publisher"), null,
            (_, _) => { calls++; return Task.CompletedTask; });

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("ok")), "orders");

        // Assert
        calls.ShouldBe(1);
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
    }

    [Test]
    public async Task HandleMessageAsync_AcknowledgementFailureDoesNotRetryConsumerOrPublishFault()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var failure = new IOException("ack failed");
        consumer.AckException = failure;
        var calls = 0;
        var handler = new RabbitMqMessageHandler(consumer.Channel,
            _ => throw new InvalidOperationException("Ack failure must not open a publisher"), null,
            (_, _) => { calls++; return Task.CompletedTask; });

        // Act
        var exception = await Should.ThrowAsync<IOException>(() =>
            handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("ok")), "orders"));

        // Assert
        exception.ShouldBeSameAs(failure);
        calls.ShouldBe(1);
        consumer.AckedDeliveryTags.ShouldBeEmpty();
    }

    [Test]
    public async Task HandleMessageAsync_WaitsForTerminalPublicationBeforeAcknowledging()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var publishing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        publisher.BeforePublishAsync = async ct =>
        {
            publishing.SetResult();
            await release.Task.WaitAsync(ct);
        };
        var handler = new RabbitMqMessageHandler(consumer.Channel, _ => Task.FromResult(publisher.Channel), null,
            (_, _) => throw new InvalidOperationException("failed"), maxRetryCount: 0);

        // Act
        var handling = handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("fail")), "orders");
        await publishing.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var acknowledgedBeforePublication = consumer.AckedDeliveryTags.ToArray();
        release.SetResult();
        await handling;

        // Assert
        acknowledgedBeforePublication.ShouldBeEmpty();
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
        publisher.DisposeCount.ShouldBe(1);
    }

    [Test]
    public async Task HandleMessageAsync_SkippedPublicationFailureLeavesDeliveryUnacknowledged()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        publisher.PublishException = new IOException("failed publish");
        var calls = 0;
        var handler = new RabbitMqMessageHandler(consumer.Channel, _ => Task.FromResult(publisher.Channel), null,
            (_, _) => { calls++; throw new MessageNotConsumedException("unhandled"); });

        // Act
        var exception = await Should.ThrowAsync<InvalidOperationException>(() =>
            handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("skip")), "orders"));

        // Assert
        exception.InnerException.ShouldBeSameAs(publisher.PublishException);
        calls.ShouldBe(1);
        consumer.AckedDeliveryTags.ShouldBeEmpty();
        publisher.DisposeCount.ShouldBe(1);
    }

    [Test]
    public async Task HandleMessageAsync_FailedDeclarationCanBeRetriedOnTheNextDelivery()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var failed = TestChannel.Publisher();
        failed.DeclareException = new IOException("failed declare");
        var next = TestChannel.Publisher();
        var publishers = new Queue<TestChannel>(new[] { failed, next });
        var handler = new RabbitMqMessageHandler(consumer.Channel,
            _ => Task.FromResult(publishers.Dequeue().Channel), null,
            (_, _) => throw new InvalidOperationException("failed"), maxRetryCount: 0);

        // Act
        var exception = await Should.ThrowAsync<InvalidOperationException>(() =>
            handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("first")), "orders"));
        var acknowledgedAfterFailure = consumer.AckedDeliveryTags.ToArray();
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("second")), "orders");

        // Assert
        exception.InnerException.ShouldBeSameAs(failed.DeclareException);
        acknowledgedAfterFailure.ShouldBeEmpty();
        next.DeclaredQueues.ShouldBe(new[] { "orders_error" });
        next.Publications.Count.ShouldBe(1);
        consumer.AckedDeliveryTags.ShouldBe(new[] { 42UL });
        failed.DisposeCount.ShouldBe(1);
    }

    [Test]
    public async Task HandleMessageAsync_ShutdownCancelsDispatchWithoutAcknowledging()
    {
        // Arrange
        using var stopping = new CancellationTokenSource();
        var consumer = TestChannel.Consumer();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new RabbitMqMessageHandler(consumer.Channel,
            _ => throw new InvalidOperationException("Cancelled delivery must not publish"), null,
            async (_, ct) => { entered.SetResult(); await Task.Delay(Timeout.Infinite, ct); },
            stoppingToken: stopping.Token);

        // Act
        var handling = handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("wait")), "orders");
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await stopping.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => handling);

        // Assert
        consumer.AckedDeliveryTags.ShouldBeEmpty();
    }

    [Test]
    public async Task HandleMessageAsync_CancellationDuringRetryDelayDoesNotDispatchAgain()
    {
        // Arrange
        using var stopping = new CancellationTokenSource();
        var consumer = TestChannel.Consumer();
        var calls = 0;
        var handler = new RabbitMqMessageHandler(consumer.Channel,
            _ => throw new InvalidOperationException("Cancelled delivery must not publish"), null,
            (_, _) => { calls++; throw new IOException("failed"); }, stoppingToken: stopping.Token);

        // Act
        var handling = handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("wait")), "orders");
        await stopping.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => handling);

        // Assert
        calls.ShouldBe(1);
        consumer.AckedDeliveryTags.ShouldBeEmpty();
    }

    [Test]
    public async Task HandleMessageAsync_CancellationDuringTerminalPublicationDoesNotAcknowledge()
    {
        // Arrange
        using var stopping = new CancellationTokenSource();
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        publisher.BeforePublishAsync = async ct => { entered.SetResult(); await Task.Delay(Timeout.Infinite, ct); };
        var handler = new RabbitMqMessageHandler(consumer.Channel, _ => Task.FromResult(publisher.Channel), null,
            (_, _) => throw new InvalidOperationException("failed"), maxRetryCount: 0, stoppingToken: stopping.Token);

        // Act
        var handling = handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("fail")), "orders");
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await stopping.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => handling);

        // Assert
        consumer.AckedDeliveryTags.ShouldBeEmpty();
        publisher.DisposeCount.ShouldBe(1);
    }
}
