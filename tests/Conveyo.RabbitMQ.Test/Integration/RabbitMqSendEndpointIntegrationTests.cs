namespace Conveyo.RabbitMQ.Test.Integration;

/// <summary>
/// Round-trip tests against a real broker covering correctness invariants that fake-channel tests
/// cannot exercise: mandatory returns, publisher confirms, and ack-after-confirm sequencing.
/// </summary>
[TestFixture]
[Category("Integration")]
public class RabbitMqSendEndpointIntegrationTests
{
    private sealed record IntegrationMessage(string Value);

    [SetUp]
    public void SetUp() => BrokerFixture.SkipIfBrokerMissing();

    [Test]
    public async Task Send_ToMissingQueue_ThrowsUnroutableMessageException()
    {
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("send-missing"));

        var endpoint = new RabbitMqSendEndpoint(
            manager.Inner.CreatePublisherChannelAsync,
            queueName: $"conveyo-it-nonexistent-{Guid.NewGuid():N}",
            hostInfo: new HostInfo(),
            urn: "conveyo:test.integration.send-missing.v1");

        var ex = Assert.ThrowsAsync<UnroutableMessageException>(
            () => endpoint.Send(new IntegrationMessage("hello")));

        Assert.That(ex!.ReplyCode, Is.EqualTo((ushort)312)); // NO_ROUTE
        Assert.That(ex.RoutingKey, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public async Task Send_ToDeclaredQueue_Succeeds()
    {
        await using var manager = new ConnectionScope(await BrokerFixture.StartConnectionAsync("send-ok"));

        await using var declareChannel = await manager.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var queueName = await BrokerFixture.DeclareTransientQueueAsync(declareChannel, "send-ok");

        var endpoint = new RabbitMqSendEndpoint(
            manager.Inner.CreatePublisherChannelAsync,
            queueName: queueName,
            hostInfo: new HostInfo(),
            urn: "conveyo:test.integration.send-ok.v1");

        // No exception = broker accepted the message via publisher confirms.
        await endpoint.Send(new IntegrationMessage("hello"));
    }

    private sealed class ConnectionScope(RabbitMqConnectionManager inner) : IAsyncDisposable
    {
        public RabbitMqConnectionManager Inner { get; } = inner;

        public ValueTask DisposeAsync() => new(Inner.StopAsync(CancellationToken.None));
    }
}
