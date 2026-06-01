namespace Conveyo.Test;

[TestFixture]
public class BusTests
{
    private sealed record ExampleEvent(string Name);

    [Test]
    public async Task Publish_DelegatesToPublishEndpointForMessageType()
    {
        var provider = new FakeEndpointProvider();
        var bus = new Bus(provider);

        await bus.Publish(new ExampleEvent("hello"));

        Assert.That(provider.PublishEndpoint.Published.Count, Is.EqualTo(1));
        Assert.That(provider.PublishEndpoint.Published[0], Is.InstanceOf<ExampleEvent>());
        Assert.That(provider.SendEndpoint.Sent, Is.Empty);
    }

    [Test]
    public async Task Send_DelegatesToSendEndpointForMessageType()
    {
        var provider = new FakeEndpointProvider();
        var bus = new Bus(provider);

        await bus.Send(new ExampleEvent("hi"));

        Assert.That(provider.SendEndpoint.Sent.Count, Is.EqualTo(1));
        Assert.That(provider.PublishEndpoint.Published, Is.Empty);
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
