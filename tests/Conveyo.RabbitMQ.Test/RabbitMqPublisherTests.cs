namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class RabbitMqPublisherTests
{
    [Test]
    public async Task Send_PublishFailureDisposesChannelAndPropagatesFailure()
    {
        // Arrange
        var channel = TestChannel.Publisher();
        channel.PublishException = new IOException("publish failed");
        var endpoint = new RabbitMqSendEndpoint(_ => Task.FromResult(channel.Channel), "orders", new HostInfo(), "test:command");

        // Act
        var exception = await Should.ThrowAsync<IOException>(() => endpoint.Send(new { Value = "test" }));

        // Assert
        exception.ShouldBeSameAs(channel.PublishException);
        channel.DisposeCount.ShouldBe(1);
    }

    [Test]
    public async Task Publish_DeclarationFailureDisposesChannelWithoutPublishing()
    {
        // Arrange
        var channel = TestChannel.Publisher();
        var failure = new IOException("declare failed");
        var endpoint = new RabbitMqPublishEndpoint(_ => Task.FromResult(channel.Channel), "test:event", new HostInfo(), "test:event",
            (_, _, _) => throw failure);

        // Act
        var exception = await Should.ThrowAsync<IOException>(() => endpoint.Publish(new { Value = "test" }));

        // Assert
        exception.ShouldBeSameAs(failure);
        channel.Publications.ShouldBeEmpty();
        channel.DisposeCount.ShouldBe(1);
    }
}
