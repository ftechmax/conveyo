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
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(b =>
        {
            b.Map<OrderPlaced>("conveyo:orders.placed.v1");
            b.AddConsumer<OrderPlacedHandler>();
        });

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        context.Consumers.ShouldContain(typeof(OrderPlacedHandler));
        context.ConsumerMessages[typeof(OrderPlacedHandler)].ShouldBe(new[] { typeof(OrderPlaced) });
        context.UrnFor(typeof(OrderPlaced)).ShouldBe("conveyo:orders.placed.v1");
        context.TypeForUrn("conveyo:orders.placed.v1").ShouldBe(typeof(OrderPlaced));
    }

    [Test]
    public void AddConveyo_ThrowsWhenConsumedMessageTypeHasNoUrn()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var ex = Should.Throw<InvalidOperationException>(() =>
            services.AddConveyo(b => b.AddConsumer<OrderPlacedHandler>()));

        // Assert
        ex.Message.ShouldContain(typeof(OrderPlaced).FullName!);
        ex.Message.ShouldContain("Map<");
    }

    [Test]
    public void UrnFor_ThrowsForUnmappedType()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddConveyo(_ => { });
        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Act
        var operation = () => context.UrnFor(typeof(OrderPlaced));

        // Assert
        Should.Throw<InvalidOperationException>(operation);
    }

    [Test]
    public void Map_RegistersExplicitUrn()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(b =>
        {
            b.Map<OrderPlaced>("conveyo:orders.placed.v1");
            b.AddConsumer<OrderPlacedHandler>();
        });

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        context.UrnFor(typeof(OrderPlaced)).ShouldBe("conveyo:orders.placed.v1");
        context.TypeForUrn("conveyo:orders.placed.v1").ShouldBe(typeof(OrderPlaced));
        context.MessageTypeLookup.ShouldNotContainKey($"urn:message:{typeof(OrderPlaced).Namespace}:{typeof(OrderPlaced).Name}");
    }

    [Test]
    public void Map_CanBeCalledAfterAddConsumer()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(b =>
        {
            b.AddConsumer<OrderPlacedHandler>();
            b.Map<OrderPlaced>("conveyo:orders.placed.v1");
        });

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        context.UrnFor(typeof(OrderPlaced)).ShouldBe("conveyo:orders.placed.v1");
    }

    [Test]
    public void Map_RejectsUrnWithInvalidCharacters()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var operation = () =>
            services.AddConveyo(b => b.Map<OrderPlaced>("conveyo:bad urn"));

        // Assert
        Should.Throw<ArgumentException>(operation);
    }

    [Test]
    public void Map_RejectsUrnLongerThan255Bytes()
    {
        // Arrange
        var services = new ServiceCollection();
        var tooLong = "conveyo:" + new string('a', 256);

        // Act
        var operation = () =>
            services.AddConveyo(b => b.Map<OrderPlaced>(tooLong));

        // Assert
        Should.Throw<ArgumentException>(operation);
    }

    [Test]
    public void Map_AcceptsUrnLeavingExactlySixBytesForFaultSuffix()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(builder => builder.Map<OrderPlaced>(new string('a', 249)));
        using var provider = services.BuildServiceProvider();

        // Assert
        provider.GetRequiredService<ConveyoContext>().UrnFor(typeof(Fault<OrderPlaced>)).Length.ShouldBe(255);
    }

    [Test]
    public void Map_ValidatesDerivedFaultUrnBeforeRegistering()
    {
        // Arrange
        var services = new ServiceCollection();
        Exception? exception = null;

        // Act
        services.AddConveyo(builder =>
        {
            builder.Map<OrderPlaced>(new string('a', 249));
            try
            {
                builder.Map<OrderPlaced>(new string('b', 250));
            }
            catch (ArgumentException ex)
            {
                exception = ex;
            }
        });
        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        exception.ShouldBeOfType<ArgumentException>();
        context.UrnFor(typeof(OrderPlaced)).ShouldBe(new string('a', 249));
        context.UrnFor(typeof(Fault<OrderPlaced>)).ShouldBe(new string('a', 249) + ".fault");
    }

    [Test]
    public void Map_ThrowsWhenUrnCollidesWithDifferentType()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var register = () => services.AddConveyo(b =>
        {
            b.Map<OrderPlaced>("conveyo:orders.shared.v1");
            b.Map<OrderShipped>("conveyo:orders.shared.v1");
        });

        // Assert
        Should.Throw<InvalidOperationException>(register);
    }

    [Test]
    public void AddConsumer_HandlesMultipleMessageTypes()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(b =>
        {
            b.Map<OrderPlaced>("conveyo:orders.placed.v1");
            b.Map<OrderShipped>("conveyo:orders.shipped.v1");
            b.AddConsumer<MultiHandler>();
        });

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        context.ConsumerMessages[typeof(MultiHandler)].ShouldBe(new[] { typeof(OrderPlaced), typeof(OrderShipped) }, ignoreOrder: true);
    }

    [Test]
    public void MapEndpointConvention_StoresUriForMessageType()
    {
        // Arrange
        var services = new ServiceCollection();
        var uri = new Uri("queue:orders");

        // Act
        services.AddConveyo(b =>
        {
            b.MapEndpointConvention<OrderPlaced>(uri);
        });

        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        context.EndpointConventions[typeof(OrderPlaced)].ShouldBe(uri);
    }

    [Test]
    public void AddConveyo_RegistersBusAndHostedService()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(_ => { });

        // Assert
        var descriptors = services.ToList();
        descriptors.Any(d => d.ServiceType == typeof(IBus) && d.ImplementationType == typeof(Bus)).ShouldBeTrue();
        descriptors.Any(d => d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService)).ShouldBeTrue();
    }

    [Test]
    public void AddConsumer_RejectsTypeWithoutConsumerInterface()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var exception = Should.Throw<ArgumentException>(() => services.AddConveyo(b => b.AddConsumer<OrderPlaced>()));

        // Assert
        exception.Message.ShouldContain("IConsumer<T>");
    }

    [Test]
    public void AddConsumer_DuplicateRegistrationHasOneScopedService()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(builder =>
        {
            builder.Map<OrderPlaced>("orders:placed.v1");
            builder.AddConsumer<OrderPlacedHandler>();
            builder.AddConsumer<OrderPlacedHandler>();
        });

        // Assert
        var descriptor = services.Where(d => d.ServiceType == typeof(OrderPlacedHandler)).Single();
        descriptor.Lifetime.ShouldBe(ServiceLifetime.Scoped);
    }

    [Test]
    public void Map_RemappingRemovesPreviousMessageAndFaultUrns()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(builder =>
        {
            builder.Map<OrderPlaced>("orders:old.v1");
            builder.Map<OrderPlaced>("orders:new.v1");
        });
        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        context.TypeForUrn("orders:old.v1").ShouldBeNull();
        context.TypeForUrn("orders:old.v1.fault").ShouldBeNull();
        context.UrnFor(typeof(OrderPlaced)).ShouldBe("orders:new.v1");
        context.UrnFor(typeof(Fault<OrderPlaced>)).ShouldBe("orders:new.v1.fault");
    }

    [Test]
    public void Map_FaultUrnCollisionLeavesPreviousMappingIntact()
    {
        // Arrange
        var services = new ServiceCollection();
        Exception? exception = null;

        // Act
        services.AddConveyo(builder =>
        {
            builder.Map<OrderPlaced>("orders:old.v1");
            builder.Map<OrderShipped>("orders:new.v1.fault");
            try
            {
                builder.Map<OrderPlaced>("orders:new.v1");
            }
            catch (InvalidOperationException ex)
            {
                exception = ex;
            }
        });
        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        exception.ShouldBeOfType<InvalidOperationException>();
        context.UrnFor(typeof(OrderPlaced)).ShouldBe("orders:old.v1");
        context.UrnFor(typeof(Fault<OrderPlaced>)).ShouldBe("orders:old.v1.fault");
        context.TypeForUrn("orders:new.v1").ShouldBeNull();
        context.TypeForUrn("orders:new.v1.fault").ShouldBe(typeof(OrderShipped));
    }

    [Test]
    public void Map_CapturedBuilderCannotChangeCompletedRegistration()
    {
        // Arrange
        var services = new ServiceCollection();
        IConveyoBuilder captured = null!;
        services.AddConveyo(builder =>
        {
            captured = builder;
            builder.Map<OrderPlaced>("orders:placed.v1");
        });
        using var provider = services.BuildServiceProvider();

        // Act
        var exception = Should.Throw<InvalidOperationException>(() => captured.Map<OrderPlaced>("orders:changed.v1"));

        // Assert
        exception.Message.ShouldContain("inside AddConveyo");
        provider.GetRequiredService<ConveyoContext>().UrnFor(typeof(OrderPlaced)).ShouldBe("orders:placed.v1");
    }

    [Test]
    public void AddConsumer_CapturedBuilderCannotAddServicesAfterRegistration()
    {
        // Arrange
        var services = new ServiceCollection();
        IConveyoBuilder captured = null!;
        services.AddConveyo(builder => captured = builder);

        // Act
        var exception = Should.Throw<InvalidOperationException>(() => captured.AddConsumer<OrderPlacedHandler>());

        // Assert
        exception.Message.ShouldContain("inside AddConveyo");
        services.ShouldNotContain(d => d.ServiceType == typeof(OrderPlacedHandler));
    }

    [Test]
    public void MessageDataLimit_CapturedBuilderCannotChangeRuntimeLimit()
    {
        // Arrange
        var services = new ServiceCollection();
        IConveyoBuilder captured = null!;
        services.AddConveyo(builder =>
        {
            captured = builder;
            builder.MaxMessageDataBytes(4);
        });
        using var provider = services.BuildServiceProvider();

        // Act
        var exception = Should.Throw<InvalidOperationException>(() => captured.MaxMessageDataBytes(8));

        // Assert
        exception.Message.ShouldContain("inside AddConveyo");
        provider.GetRequiredService<ConveyoContext>().MaxMessageDataBytes.ShouldBe(4);
    }

    [Test]
    public void Registration_RuntimeCollectionsRejectMutation()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddConveyo(builder =>
        {
            builder.Map<OrderPlaced>("orders:placed.v1");
            builder.AddConsumer<OrderPlacedHandler>();
        });
        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Act
        var replaceMapping = () => ((IDictionary<Type, string>)context.UrnsByType)[typeof(OrderPlaced)] = "changed";
        var addMessage = () => ((IList<Type>)context.ConsumerMessages[typeof(OrderPlacedHandler)]).Add(typeof(OrderShipped));

        // Assert
        Should.Throw<NotSupportedException>(replaceMapping);
        Should.Throw<NotSupportedException>(addMessage);
    }
    [Test]
    public void Map_RemappingCanReusePreviousMessageUrnForItsFault()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(builder =>
        {
            builder.Map<OrderPlaced>("orders:placed.v1.fault");
            builder.Map<OrderPlaced>("orders:placed.v1");
        });
        using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<ConveyoContext>();

        // Assert
        context.TypeForUrn("orders:placed.v1").ShouldBe(typeof(OrderPlaced));
        context.TypeForUrn("orders:placed.v1.fault").ShouldBe(typeof(Fault<OrderPlaced>));
        context.TypeForUrn("orders:placed.v1.fault.fault").ShouldBeNull();
    }

}
