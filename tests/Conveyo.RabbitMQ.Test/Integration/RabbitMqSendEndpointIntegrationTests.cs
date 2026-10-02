namespace Conveyo.RabbitMQ.Test.Integration;

/// <summary>
/// Checks sends to declared and missing queues against a RabbitMQ broker.
/// </summary>
[TestFixture]
[Category("Integration")]
public class RabbitMqSendEndpointIntegrationTests
{
    private sealed record IntegrationMessage(string Value);

    [Test]
    public async Task Send_ToMissingQueue_ThrowsUnroutableMessageException()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("send-missing"));
        var endpoint = new RabbitMqSendEndpoint(
            manager.Inner.CreatePublisherChannelAsync,
            queueName: $"conveyo-it-nonexistent-{Guid.NewGuid():N}",
            hostInfo: new HostInfo(),
            urn: "conveyo:test.integration.send-missing.v1");

        // Act
        var exception = await Should.ThrowAsync<UnroutableMessageException>(
            () => endpoint.Send(new IntegrationMessage("hello")));

        // Assert
        exception.ReplyCode.ShouldBe((ushort)312); // NO_ROUTE
        exception.RoutingKey.ShouldNotBeNullOrEmpty();
    }

    [Test]
    public async Task Send_ToDeclaredQueue_Succeeds()
    {
        // Arrange
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("send-ok"));
        await using var declareChannel = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var queueName = await BrokerFixture.DeclareTransientQueueAsync(declareChannel, "send-ok");
        var endpoint = new RabbitMqSendEndpoint(
            manager.Inner.CreatePublisherChannelAsync,
            queueName: queueName,
            hostInfo: new HostInfo(),
            urn: "conveyo:test.integration.send-ok.v1");

        // Act
        var send = () => endpoint.Send(new IntegrationMessage("hello"));

        // Assert
        // No exception = broker accepted the message via publisher confirms.
        await Should.NotThrowAsync(send);
    }
}
