namespace Conveyo.Test;

[TestFixture]
public class BusTests
{
    private sealed record ExampleEvent(string Name);

    [Test]
    public async Task Publish_DelegatesToPublishEndpointForMessageType()
    {
        // Arrange
        var provider = new FakeEndpointProvider();
        var bus = new Bus(provider);

        // Act
        await bus.Publish(new ExampleEvent("hello"));

        // Assert
        provider.PublishEndpoint.Published.Count.ShouldBe(1);
        provider.PublishEndpoint.Published[0].ShouldBeOfType<ExampleEvent>();
        provider.SendEndpoint.Sent.ShouldBeEmpty();
    }

    [Test]
    public async Task Send_DelegatesToSendEndpointForMessageType()
    {
        // Arrange
        var provider = new FakeEndpointProvider();
        var bus = new Bus(provider);

        // Act
        await bus.Send(new ExampleEvent("hi"));

        // Assert
        provider.SendEndpoint.Sent.Count.ShouldBe(1);
        provider.PublishEndpoint.Published.ShouldBeEmpty();
    }

    private sealed class FakeEndpointProvider : IEndpointProvider
    {
        public FakePublishEndpoint PublishEndpoint { get; } = new();
        public FakeSendEndpoint SendEndpoint { get; } = new();

        public IPublishEndpoint GetPublishEndpoint<T>() where T : class => PublishEndpoint;
        public ISendEndpoint GetSendEndpoint<T>() where T : class => SendEndpoint;
        public ISendEndpoint GetSendEndpoint<T>(Uri address) where T : class => SendEndpoint;
    }

    private sealed class FakePublishEndpoint : IPublishEndpoint
    {
        public List<object> Published { get; } = new();
        public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class
        {
            Published.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeSendEndpoint : ISendEndpoint
    {
        public List<object> Sent { get; } = new();
        public Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class
        {
            Sent.Add(message);
            return Task.CompletedTask;
        }
    }

}
