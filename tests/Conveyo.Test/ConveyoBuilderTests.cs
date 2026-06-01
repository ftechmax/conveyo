using Microsoft.Extensions.DependencyInjection;

namespace Conveyo.Test;

[TestFixture]
public class ConveyoBuilderTests
{
    private sealed record OrderPlaced(Guid Id);
    private sealed record OrderShipped(Guid Id);

    private sealed class OrderPlacedHandler : IConsumer<OrderPlaced>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context) => Task.CompletedTask;
    }

    private sealed class MultiHandler : IConsumer<OrderPlaced>, IConsumer<OrderShipped>
    {
        public Task Consume(ConsumeContext<OrderPlaced> context) => Task.CompletedTask;
        public Task Consume(ConsumeContext<OrderShipped> context) => Task.CompletedTask;
    }

    [Test]
    public void AddConsumer_RegistersHandlerAndMessageMapping()
    {
        var services = new ServiceCollection();
        ConveyoContext context = null!;
        services.AddConveyo(b =>
        {
            b.Map<OrderPlaced>("conveyo:orders.placed.v1");
            b.AddConsumer<OrderPlacedHandler>();
            context = b.Context;
        });

        Assert.That(context.Consumers, Does.Contain(typeof(OrderPlacedHandler)));
        Assert.That(context.ConsumerMessages[typeof(OrderPlacedHandler)], Is.EqualTo(new[] { typeof(OrderPlaced) }));
        Assert.That(context.UrnFor(typeof(OrderPlaced)), Is.EqualTo("conveyo:orders.placed.v1"));
        Assert.That(context.TypeForUrn("conveyo:orders.placed.v1"), Is.EqualTo(typeof(OrderPlaced)));
    }

    [Test]
    public void AddConveyo_ThrowsWhenConsumedMessageTypeHasNoUrn()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddConveyo(b => b.AddConsumer<OrderPlacedHandler>()));

        Assert.That(ex!.Message, Does.Contain(typeof(OrderPlaced).FullName!));
        Assert.That(ex.Message, Does.Contain("Map<"));
    }

    [Test]
    public void UrnFor_ThrowsForUnmappedType()
    {
        var context = new ConveyoContext { HostInfo = new HostInfo() };

        Assert.Throws<InvalidOperationException>(() => context.UrnFor(typeof(OrderPlaced)));
    }

    [Test]
    public void Map_RegistersExplicitUrn()
    {
        var services = new ServiceCollection();
        ConveyoContext context = null!;
        services.AddConveyo(b =>
        {
            b.Map<OrderPlaced>("conveyo:orders.placed.v1");
            b.AddConsumer<OrderPlacedHandler>();
            context = b.Context;
        });

        Assert.That(context.UrnFor(typeof(OrderPlaced)), Is.EqualTo("conveyo:orders.placed.v1"));
        Assert.That(context.TypeForUrn("conveyo:orders.placed.v1"), Is.EqualTo(typeof(OrderPlaced)));
        Assert.That(context.MessageTypeLookup, Does.Not.ContainKey($"urn:message:{typeof(OrderPlaced).Namespace}:{typeof(OrderPlaced).Name}"));
    }

    [Test]
    public void Map_CanBeCalledAfterAddConsumer()
    {
        var services = new ServiceCollection();
        ConveyoContext context = null!;
        services.AddConveyo(b =>
        {
            b.AddConsumer<OrderPlacedHandler>();
            b.Map<OrderPlaced>("conveyo:orders.placed.v1");
            context = b.Context;
        });

        Assert.That(context.UrnFor(typeof(OrderPlaced)), Is.EqualTo("conveyo:orders.placed.v1"));
    }

    [Test]
    public void Map_RejectsUrnWithInvalidCharacters()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() =>
            services.AddConveyo(b => b.Map<OrderPlaced>("conveyo:bad urn")));
    }

    [Test]
    public void Map_RejectsUrnLongerThan255Bytes()
    {
        var services = new ServiceCollection();
        var tooLong = "conveyo:" + new string('a', 256);

        Assert.Throws<ArgumentException>(() =>
            services.AddConveyo(b => b.Map<OrderPlaced>(tooLong)));
    }

    [Test]
    public void Map_ThrowsWhenUrnCollidesWithDifferentType()
    {
        var services = new ServiceCollection();

        Assert.Throws<InvalidOperationException>(() =>
            services.AddConveyo(b =>
            {
                b.Map<OrderPlaced>("conveyo:orders.shared.v1");
                b.Map<OrderShipped>("conveyo:orders.shared.v1");
            }));
    }

    [Test]
    public void AddConsumer_HandlesMultipleMessageTypes()
    {
        var services = new ServiceCollection();
        ConveyoContext context = null!;
        services.AddConveyo(b =>
        {
            b.Map<OrderPlaced>("conveyo:orders.placed.v1");
            b.Map<OrderShipped>("conveyo:orders.shipped.v1");
            b.AddConsumer<MultiHandler>();
            context = b.Context;
        });

        Assert.That(context.ConsumerMessages[typeof(MultiHandler)],
            Is.EquivalentTo(new[] { typeof(OrderPlaced), typeof(OrderShipped) }));
    }

    [Test]
    public void MapEndpointConvention_StoresUriForMessageType()
    {
        var services = new ServiceCollection();
        var uri = new Uri("queue:orders");
        ConveyoContext context = null!;

        services.AddConveyo(b =>
        {
            b.MapEndpointConvention<OrderPlaced>(uri);
            context = b.Context;
        });

        Assert.That(context.EndpointConventions[typeof(OrderPlaced)], Is.EqualTo(uri));
    }

    [Test]
    public void AddConveyo_RegistersBusAndHostedService()
    {
        var services = new ServiceCollection();
        services.AddConveyo(_ => { });

        var descriptors = services.ToList();
        Assert.That(descriptors.Any(d => d.ServiceType == typeof(IBus) && d.ImplementationType == typeof(Bus)), Is.True);
        Assert.That(descriptors.Any(d => d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService)), Is.True);
    }
}
