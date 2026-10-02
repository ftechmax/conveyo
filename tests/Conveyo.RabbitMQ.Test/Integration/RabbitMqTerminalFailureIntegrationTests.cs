using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Conveyo.RabbitMQ.Test.Integration;

public partial class RabbitMqMessageHandlerIntegrationTests
{
    [Test]
    public async Task TerminalReturn_LeavesOriginalUnacknowledgedAndRedeliverable()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("terminal-return"));
        await using var setup = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var queue = await BrokerFixture.DeclareTransientQueueAsync(setup, "terminal-return");
        var delivery = await PublishAndReceiveAsync(manager.Inner, setup, queue,
            new IntegrationMessage("returned"), "test:terminal-return");
        var handler = new RabbitMqMessageHandler(manager.Inner.ConsumerChannel!, async ct =>
            new DeleteTerminalQueueChannel(await manager.Inner.CreatePublisherChannelAsync(ct), setup), null,
            (_, _) => throw new InvalidOperationException("consumer failed"), maxRetryCount: 0);

        // Act
        var failure = await Should.ThrowAsync<InvalidOperationException>(() => handler.HandleMessageAsync(delivery, queue));
        await manager.Inner.ConsumerChannel!.CloseAsync(CancellationToken.None);
        var redelivery = await setup.BasicGetAsync(queue, autoAck: true);

        // Assert
        failure.InnerException.ShouldBeOfType<PublishReturnException>();
        redelivery.ShouldNotBeNull();
        redelivery.Redelivered.ShouldBeTrue();
        redelivery.Body.ToArray().ShouldBe(delivery.Body.ToArray());
    }

    [Test]
    public async Task TerminalDeclarationFailure_LeavesOriginalUnacknowledgedAndRedeliverable()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("terminal-conflict"));
        await using var setup = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var queue = await BrokerFixture.DeclareTransientQueueAsync(setup, "terminal-conflict");
        var errorQueue = $"{queue}_error";
        // A durable queue with different auto-delete semantics is valid on RabbitMQ 4.3,
        // and conflicts with Conveyo's terminal declaration.
        await setup.QueueDeclareAsync(errorQueue, durable: true, exclusive: false, autoDelete: true);
        var delivery = await PublishAndReceiveAsync(manager.Inner, setup, queue,
            new IntegrationMessage("conflict"), "test:terminal-conflict");
        var handler = new RabbitMqMessageHandler(manager.Inner.ConsumerChannel!, manager.Inner.CreatePublisherChannelAsync,
            null, (_, _) => throw new InvalidOperationException("consumer failed"), maxRetryCount: 0);

        // Act
        var failure = await Should.ThrowAsync<InvalidOperationException>(() => handler.HandleMessageAsync(delivery, queue));
        await manager.Inner.ConsumerChannel!.CloseAsync(CancellationToken.None);
        var redelivery = await setup.BasicGetAsync(queue, autoAck: true);

        // Assert
        failure.InnerException.ShouldBeOfType<OperationInterruptedException>();
        redelivery.ShouldNotBeNull();
        redelivery.Redelivered.ShouldBeTrue();
        await DeleteQueueAsync(setup, errorQueue);
    }

    [Test]
    public async Task TerminalConnectionLoss_LeavesOriginalUnacknowledgedAndRedeliverable()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("terminal-disconnect"));
        await using var setup = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var queue = await BrokerFixture.DeclareTransientQueueAsync(setup, "terminal-disconnect");
        var delivery = await PublishAndReceiveAsync(manager.Inner, setup, queue,
            new IntegrationMessage("disconnect"), "test:terminal-disconnect");
        var publishing = await BrokerFixture.StartConnectionAsync("failed-terminal-publisher");
        var handler = new RabbitMqMessageHandler(manager.Inner.ConsumerChannel!, async ct =>
            new DisconnectTerminalChannel(await publishing.CreatePublisherChannelAsync(ct), publishing), null,
            (_, _) => throw new InvalidOperationException("consumer failed"), maxRetryCount: 0);
        try
        {
            // Act
            var failure = await Should.ThrowAsync<InvalidOperationException>(() => handler.HandleMessageAsync(delivery, queue));
            await manager.Inner.ConsumerChannel!.CloseAsync(CancellationToken.None);
            var redelivery = await setup.BasicGetAsync(queue, autoAck: true);

            // Assert
            failure.InnerException.ShouldBeOfType<AlreadyClosedException>();
            redelivery.ShouldNotBeNull();
            redelivery.Redelivered.ShouldBeTrue();
        }
        finally
        {
            await publishing.StopAsync(CancellationToken.None);
            await DeleteQueueAsync(setup, $"{queue}_error");
        }
    }

    [Test]
    public async Task MalformedEnvelope_PublishesOriginalBodyAndAcknowledges()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("malformed"));
        await using var setup = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var queue = await BrokerFixture.DeclareTransientQueueAsync(setup, "malformed");
        await setup.BasicPublishAsync(string.Empty, queue, true, new BasicProperties(), "{"u8.ToArray());
        var input = await manager.Inner.ConsumerChannel!.BasicGetAsync(queue, autoAck: false);
        input.ShouldNotBeNull();
        var delivery = new global::RabbitMQ.Client.Events.BasicDeliverEventArgs("test", input.DeliveryTag, false,
            input.Exchange, input.RoutingKey, input.BasicProperties, input.Body.ToArray());
        var handler = new RabbitMqMessageHandler(manager.Inner.ConsumerChannel!, manager.Inner.CreatePublisherChannelAsync,
            null, (_, _) => throw new InvalidOperationException("Invalid envelope must not dispatch"));

        // Act
        await handler.HandleMessageAsync(delivery, queue);
        await manager.Inner.ConsumerChannel!.CloseAsync(CancellationToken.None);

        // Assert
        var terminal = await setup.BasicGetAsync($"{queue}_error", autoAck: true);
        terminal.ShouldNotBeNull();
        terminal.Body.ToArray().ShouldBe("{"u8.ToArray());
        HeaderText(terminal, RabbitMqMessageHandler.FaultReasonHeader).ShouldBe("deserialization-failed");
        (await setup.BasicGetAsync(queue, autoAck: true)).ShouldBeNull();
        await DeleteQueueAsync(setup, $"{queue}_error");
    }

    [Test]
    public async Task OversizedEnvelope_PublishesEmptyBodyAndAcknowledges()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("oversized"));
        await using var setup = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var queue = await BrokerFixture.DeclareTransientQueueAsync(setup, "oversized");
        var delivery = await PublishAndReceiveAsync(manager.Inner, setup, queue,
            new IntegrationMessage("oversized"), "test:oversized");
        var handler = new RabbitMqMessageHandler(manager.Inner.ConsumerChannel!, manager.Inner.CreatePublisherChannelAsync,
            null, (_, _) => throw new InvalidOperationException("Oversized envelope must not dispatch"), maxEnvelopeSizeBytes: 1);

        // Act
        await handler.HandleMessageAsync(delivery, queue);
        await manager.Inner.ConsumerChannel!.CloseAsync(CancellationToken.None);

        // Assert
        var terminal = await setup.BasicGetAsync($"{queue}_error", autoAck: true);
        terminal.ShouldNotBeNull();
        terminal.Body.IsEmpty.ShouldBeTrue();
        HeaderText(terminal, RabbitMqMessageHandler.FaultReasonHeader).ShouldBe("envelope-too-large");
        HeaderText(terminal, RabbitMqMessageHandler.FaultExceptionMessageHeader).ShouldBe(ExceptionInfo.RedactedMessage);
        (await setup.BasicGetAsync(queue, autoAck: true)).ShouldBeNull();
        await DeleteQueueAsync(setup, $"{queue}_error");
    }

    // These adapters delegate declares, publishes, confirms, and disposal to actual broker
    // channels. Only the interruption between declaration and publication is controlled.
    private sealed class DeleteTerminalQueueChannel(IChannel inner, IChannel setup) : UnexpectedChannel
    {
        public override async Task<QueueDeclareOk> QueueDeclareAsync(string queue, bool durable, bool exclusive,
            bool autoDelete, IDictionary<string, object?>? arguments, bool passive, bool noWait, CancellationToken cancellationToken)
        {
            var result = await inner.QueueDeclareAsync(queue, durable, exclusive, autoDelete, arguments, passive, noWait, cancellationToken);
            await setup.QueueDeleteAsync(queue, cancellationToken: cancellationToken);
            return result;
        }
        public override ValueTask BasicPublishAsync<TProperties>(string exchange, string routingKey, bool mandatory,
            TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
            => inner.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body, cancellationToken);
        public override ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private sealed class DisconnectTerminalChannel(IChannel inner, RabbitMqConnectionManager manager) : UnexpectedChannel
    {
        public override async Task<QueueDeclareOk> QueueDeclareAsync(string queue, bool durable, bool exclusive,
            bool autoDelete, IDictionary<string, object?>? arguments, bool passive, bool noWait, CancellationToken cancellationToken)
        {
            var result = await inner.QueueDeclareAsync(queue, durable, exclusive, autoDelete, arguments, passive, noWait, cancellationToken);
            await manager.StopAsync(cancellationToken);
            return result;
        }
        public override ValueTask BasicPublishAsync<TProperties>(string exchange, string routingKey, bool mandatory,
            TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
            => inner.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body, cancellationToken);
        public override ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
