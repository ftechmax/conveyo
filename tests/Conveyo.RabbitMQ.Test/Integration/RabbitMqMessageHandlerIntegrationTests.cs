using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ.Test.Integration;

/// <summary>
/// Checks error and skipped routing, original-delivery acknowledgement, and terminal
/// message contents against a RabbitMQ broker.
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class RabbitMqMessageHandlerIntegrationTests
{
    private sealed record IntegrationMessage(string Value);

    [Test]
    public async Task ConsumerException_PublishesToErrorQueue_BeforeAck_AndIsReceivable()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("error-publish"));
        await using var setupChannel = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var inputQueue = await BrokerFixture.DeclareTransientQueueAsync(setupChannel, "in");
        var errorQueue = $"{inputQueue}_error";
        var delivery = await PublishAndReceiveAsync(
            manager.Inner, setupChannel, inputQueue, new IntegrationMessage("boom"), "conveyo:test.integration.error.v1");
        var attempts = 0;
        var handler = new RabbitMqMessageHandler(
            manager.Inner.ConsumerChannel!,
            manager.Inner.CreatePublisherChannelAsync,
            logger: null,
            onMessageAsync: (_, _) => { attempts++; throw new InvalidOperationException("consumer always fails"); },
            maxRetryCount: 1);

        // Act
        await handler.HandleMessageAsync(delivery, inputQueue);

        // Assert
        attempts.ShouldBe(2);
        await manager.Inner.ConsumerChannel!.CloseAsync(CancellationToken.None);
        (await setupChannel.BasicGetAsync(inputQueue, autoAck: true)).ShouldBeNull();
        // Retrieve the terminal copy to prove the broker accepted it.
        var faulted = await setupChannel.BasicGetAsync(errorQueue, autoAck: true, CancellationToken.None);
        faulted.ShouldNotBeNull("error queue did not receive the failed message");
        HeaderText(faulted, "conveyo-outcome").ShouldBe("faulted");
        HeaderText(faulted, "conveyo-fault-reason").ShouldBe("exception");
        HeaderText(faulted, "conveyo-fault-attempts").ShouldBe("2");

        await DeleteQueueAsync(setupChannel, errorQueue);
    }

    [Test]
    public async Task MessageNotConsumed_PublishesToSkippedQueue_BeforeAck_AndIsReceivable()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("skip-publish"));
        await using var setupChannel = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var inputQueue = await BrokerFixture.DeclareTransientQueueAsync(setupChannel, "in");
        var skippedQueue = $"{inputQueue}_skipped";
        var delivery = await PublishAndReceiveAsync(
            manager.Inner, setupChannel, inputQueue, new IntegrationMessage("skip-me"), "conveyo:test.integration.skip.v1");
        var handler = new RabbitMqMessageHandler(
            manager.Inner.ConsumerChannel!,
            manager.Inner.CreatePublisherChannelAsync,
            logger: null,
            onMessageAsync: (_, _) => throw new MessageNotConsumedException("no consumer registered"));

        // Act
        await handler.HandleMessageAsync(delivery, inputQueue);

        // Assert
        await manager.Inner.ConsumerChannel!.CloseAsync(CancellationToken.None);
        (await setupChannel.BasicGetAsync(inputQueue, autoAck: true)).ShouldBeNull();
        var skipped = await setupChannel.BasicGetAsync(skippedQueue, autoAck: true, CancellationToken.None);
        skipped.ShouldNotBeNull("skipped queue did not receive the message");
        HeaderText(skipped, "conveyo-outcome").ShouldBe("skipped");

        await DeleteQueueAsync(setupChannel, skippedQueue);
    }

    [Test]
    public async Task DeletedErrorQueue_IsRedeclaredBeforeNextTerminalPublish()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("lazy-error-q"));
        await using var setupChannel = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var inputQueue = await BrokerFixture.DeclareTransientQueueAsync(setupChannel, "in");
        var errorQueue = $"{inputQueue}_error";
        var delivery = await PublishAndReceiveAsync(
            manager.Inner, setupChannel, inputQueue, new IntegrationMessage("orphan"), "conveyo:test.integration.missing-err.v1");
        var handler = new RabbitMqMessageHandler(
            manager.Inner.ConsumerChannel!,
            manager.Inner.CreatePublisherChannelAsync,
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer always fails"),
            maxRetryCount: 0);

        await handler.HandleMessageAsync(delivery, inputQueue);
        await DeleteQueueAsync(setupChannel, errorQueue);
        var nextDelivery = await PublishAndReceiveAsync(manager.Inner, setupChannel, inputQueue,
            new IntegrationMessage("second"), "conveyo:test.integration.missing-err.v1");

        // Act
        await handler.HandleMessageAsync(nextDelivery, inputQueue);

        // Assert
        var faulted = await setupChannel.BasicGetAsync(errorQueue, autoAck: true, CancellationToken.None);
        faulted.ShouldNotBeNull("recreated error queue did not receive the second failed message");
        faulted.Body.ToArray().ShouldBe(nextDelivery.Body.ToArray());
        await manager.Inner.ConsumerChannel!.CloseAsync(CancellationToken.None);
        (await setupChannel.BasicGetAsync(inputQueue, autoAck: true)).ShouldBeNull();
        await DeleteQueueAsync(setupChannel, errorQueue);
    }

    /// <summary>
    /// Publishes the message to the input queue and returns the unacknowledged delivery the broker
    /// hands to the consumer channel.
    /// </summary>
    private static async Task<BasicDeliverEventArgs> PublishAndReceiveAsync(
        RabbitMqConnectionManager manager,
        IChannel publishChannel,
        string inputQueue,
        IntegrationMessage message,
        string urn)
    {
        var envelope = EnvelopeSerializer.Create(message, new HostInfo(), urn);
        await publishChannel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: inputQueue,
            mandatory: true,
            basicProperties: RabbitMqMessageProperties.ForEnvelope(envelope),
            body: EnvelopeSerializer.Serialize(envelope),
            cancellationToken: CancellationToken.None);

        var delivery = await manager.ConsumerChannel!.BasicGetAsync(inputQueue, autoAck: false);
        delivery.ShouldNotBeNull();
        return new BasicDeliverEventArgs("test", delivery.DeliveryTag, delivery.Redelivered,
            delivery.Exchange, delivery.RoutingKey, delivery.BasicProperties, delivery.Body.ToArray());
    }

    private static string HeaderText(BasicGetResult message, string header)
    {
        var headers = message.BasicProperties.Headers;
        headers.ShouldNotBeNull();
        return Encoding.UTF8.GetString((byte[])headers[header]!);
    }

    private static Task DeleteQueueAsync(IChannel channel, string queueName)
        => channel.QueueDeleteAsync(
            queue: queueName,
            ifUnused: false,
            ifEmpty: false,
            cancellationToken: CancellationToken.None);
}
