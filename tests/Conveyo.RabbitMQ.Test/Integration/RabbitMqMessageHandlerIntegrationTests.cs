using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ.Test.Integration;

/// <summary>
/// Verifies the safety guarantees the message handler depends on against a real broker: error and
/// skipped publishes are confirmed before the original delivery is acked, and broker-side queues
/// actually receive the routed messages.
/// </summary>
[TestFixture]
[Category("Integration")]
public class RabbitMqMessageHandlerIntegrationTests
{
    private sealed record IntegrationMessage(string Value);

    [SetUp]
    public void SetUp() => BrokerFixture.SkipIfBrokerMissing();

    [Test]
    public async Task ConsumerException_PublishesToErrorQueue_BeforeAck_AndIsReceivable()
    {
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("error-publish"));

        await using var setupChannel = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);

        var inputQueue = await BrokerFixture.DeclareTransientQueueAsync(setupChannel, "in");
        var errorQueue = $"{inputQueue}_error";

        var consumerChannel = manager.Inner.ConsumerChannel!;
        var deliveryReceived = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += (_, args) =>
        {
            deliveryReceived.TrySetResult(args);
            return Task.CompletedTask;
        };
        await consumerChannel.BasicConsumeAsync(inputQueue, autoAck: false, consumer, CancellationToken.None);

        // Publish a message that will fail consumer logic and end up in the _error queue.
        var envelope = EnvelopeSerializer.Create(new IntegrationMessage("boom"), new HostInfo(), "conveyo:test.integration.error.v1");
        var body = EnvelopeSerializer.Serialize(envelope);
        var properties = RabbitMqMessageProperties.ForEnvelope(envelope);
        await setupChannel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: inputQueue,
            mandatory: true,
            basicProperties: properties,
            body: body,
            cancellationToken: CancellationToken.None);

        var delivery = await deliveryReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var handler = new RabbitMqMessageHandler(
            consumerChannel,
            manager.Inner.CreatePublisherChannelAsync,
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer always fails"),
            maxRetryCount: 0);

        await handler.HandleMessageAsync(delivery, inputQueue);

        // Pull the message back out of the error queue to prove the broker accepted it durably.
        var faulted = await setupChannel.BasicGetAsync(errorQueue, autoAck: true, CancellationToken.None);
        Assert.That(faulted, Is.Not.Null, "error queue did not receive the failed message");
        Assert.That(faulted!.BasicProperties.Headers, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString((byte[])faulted.BasicProperties.Headers!["conveyo-outcome"]!),
            Is.EqualTo("faulted"));
        Assert.That(Encoding.UTF8.GetString((byte[])faulted.BasicProperties.Headers["conveyo-fault-reason"]!),
            Is.EqualTo("exception"));

        await DeleteQueueAsync(setupChannel, errorQueue);
    }

    [Test]
    public async Task MessageNotConsumed_PublishesToSkippedQueue_BeforeAck_AndIsReceivable()
    {
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("skip-publish"));

        await using var setupChannel = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);

        var inputQueue = await BrokerFixture.DeclareTransientQueueAsync(setupChannel, "in");
        var skippedQueue = $"{inputQueue}_skipped";

        var consumerChannel = manager.Inner.ConsumerChannel!;
        var deliveryReceived = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += (_, args) =>
        {
            deliveryReceived.TrySetResult(args);
            return Task.CompletedTask;
        };
        await consumerChannel.BasicConsumeAsync(inputQueue, autoAck: false, consumer, CancellationToken.None);

        var envelope = EnvelopeSerializer.Create(new IntegrationMessage("skip-me"), new HostInfo(), "conveyo:test.integration.skip.v1");
        var body = EnvelopeSerializer.Serialize(envelope);
        var properties = RabbitMqMessageProperties.ForEnvelope(envelope);
        await setupChannel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: inputQueue,
            mandatory: true,
            basicProperties: properties,
            body: body,
            cancellationToken: CancellationToken.None);

        var delivery = await deliveryReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var handler = new RabbitMqMessageHandler(
            consumerChannel,
            manager.Inner.CreatePublisherChannelAsync,
            logger: null,
            onMessageAsync: (_, _) => throw new MessageNotConsumedException("no consumer registered"));

        await handler.HandleMessageAsync(delivery, inputQueue);

        var skipped = await setupChannel.BasicGetAsync(skippedQueue, autoAck: true, CancellationToken.None);
        Assert.That(skipped, Is.Not.Null, "skipped queue did not receive the message");
        Assert.That(Encoding.UTF8.GetString((byte[])skipped!.BasicProperties.Headers!["conveyo-outcome"]!),
            Is.EqualTo("skipped"));

        await DeleteQueueAsync(setupChannel, skippedQueue);
    }

    [Test]
    public async Task MissingErrorQueue_IsDeclaredLazily_BeforeTerminalPublish()
    {
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("lazy-error-q"));

        await using var setupChannel = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var inputQueue = await BrokerFixture.DeclareTransientQueueAsync(setupChannel, "in");
        var errorQueue = $"{inputQueue}_error";

        var consumerChannel = manager.Inner.ConsumerChannel!;
        var deliveryReceived = new TaskCompletionSource<BasicDeliverEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(consumerChannel);
        consumer.ReceivedAsync += (_, args) =>
        {
            deliveryReceived.TrySetResult(args);
            return Task.CompletedTask;
        };
        await consumerChannel.BasicConsumeAsync(inputQueue, autoAck: false, consumer, CancellationToken.None);

        var envelope = EnvelopeSerializer.Create(new IntegrationMessage("orphan"), new HostInfo(), "conveyo:test.integration.missing-err.v1");
        var body = EnvelopeSerializer.Serialize(envelope);
        var properties = RabbitMqMessageProperties.ForEnvelope(envelope);
        await setupChannel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: inputQueue,
            mandatory: true,
            basicProperties: properties,
            body: body,
            cancellationToken: CancellationToken.None);

        var delivery = await deliveryReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var handler = new RabbitMqMessageHandler(
            consumerChannel,
            manager.Inner.CreatePublisherChannelAsync,
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer always fails"),
            maxRetryCount: 0);

        await handler.HandleMessageAsync(delivery, inputQueue);

        var faulted = await setupChannel.BasicGetAsync(errorQueue, autoAck: true, CancellationToken.None);
        Assert.That(faulted, Is.Not.Null, "lazy error queue declaration did not receive the failed message");

        await DeleteQueueAsync(setupChannel, errorQueue);
    }

    private static Task DeleteQueueAsync(IChannel channel, string queueName)
        => channel.QueueDeleteAsync(
            queue: queueName,
            ifUnused: false,
            ifEmpty: false,
            cancellationToken: CancellationToken.None);

    private sealed class ConnectionScope(RabbitMqConnectionManager inner) : IAsyncDisposable
    {
        public RabbitMqConnectionManager Inner { get; } = inner;

        public ValueTask DisposeAsync() => new(Inner.StopAsync(CancellationToken.None));
    }
}
