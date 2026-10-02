using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class RabbitMqRegistrationTests
{
    [Test]
    public void ReceiveEndpoint_RejectsConsumerNotAddedToCore()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var exception = Should.Throw<InvalidOperationException>(() => services.AddConveyo(builder =>
        {
            builder.Map<Command>("test:command.v1");
            builder.UsingRabbitMq((context, rabbit) =>
            {
                rabbit.Host("localhost", "/", _ => { });
                rabbit.ReceiveEndpoint("commands", endpoint => endpoint.ConfigureConsumer<Consumer>(context));
            });
        }));

        // Assert
        exception.Message.ShouldContain("AddConsumer<");
    }

    [Test]
    public void ReceiveEndpoint_AllowsCoreRegistrationAfterTransportConfiguration()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddConveyo(builder =>
        {
            builder.UsingRabbitMq((context, rabbit) =>
            {
                rabbit.Host("localhost", "/", _ => { });
                rabbit.ReceiveEndpoint("commands", endpoint => endpoint.ConfigureConsumer<Consumer>(context));
            });
            builder.AddConsumer<Consumer>();
            builder.Map<Command>("test:command.v1");
        });
        using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<RabbitMqBusRegistrationContext>();

        // Assert
        transport.GetQueueName(typeof(Command)).ShouldBe("commands");
        transport.GetExchangeName(typeof(Command)).ShouldBe("test:command.v1");
    }

    [Test]
    public void RegisterConsumer_CapturedContextRejectsRegistrationAfterCallback()
    {
        // Arrange
        IRabbitMqBusRegistrationContext captured = null!;
        using var provider = Build(builder => builder.UsingRabbitMq((context, rabbit) =>
        {
            captured = context;
            rabbit.Host("localhost", "/", _ => { });
            rabbit.ReceiveEndpoint("commands", endpoint => endpoint.ConfigureConsumer<Consumer>(context));
        }));

        // Act
        var exception = Should.Throw<InvalidOperationException>(() => captured.RegisterConsumer<Consumer>("late"));

        // Assert
        exception.Message.ShouldContain("inside AddConveyo");
        provider.GetRequiredService<RabbitMqBusRegistrationContext>().GetQueueName(typeof(Command)).ShouldBe("commands");
    }

    [Test]
    public void GetQueueName_DuplicateEndpointRegistrationKeepsSingleTarget()
    {
        // Arrange
        using var provider = Build(builder => builder.UsingRabbitMq((context, rabbit) =>
        {
            rabbit.Host("localhost", "/", _ => { });
            rabbit.ReceiveEndpoint("commands", endpoint =>
            {
                endpoint.ConfigureConsumer<Consumer>(context);
                endpoint.ConfigureConsumer<Consumer>(context);
            });
        }));

        // Act
        var queue = provider.GetRequiredService<RabbitMqBusRegistrationContext>().GetQueueName(typeof(Command));

        // Assert
        queue.ShouldBe("commands");
        provider.GetRequiredService<ConveyoContext>().ConsumerEndpoints[typeof(Consumer)].Count.ShouldBe(1);
    }

    [Test]
    public void GetQueueName_MultipleQueuesRequireExplicitConvention()
    {
        // Arrange
        using var provider = Build(ConfigureTwoQueues);

        // Act
        var exception = Should.Throw<InvalidOperationException>(() =>
            provider.GetRequiredService<RabbitMqBusRegistrationContext>().GetQueueName(typeof(Command)));

        // Assert
        exception.Message.ShouldContain("Commands");
        exception.Message.ShouldContain("commands");
    }

    [Test]
    public void GetQueueName_ExplicitConventionSelectsTargetAmongMultipleQueues()
    {
        // Arrange
        using var provider = Build(builder =>
        {
            ConfigureTwoQueues(builder);
            builder.MapEndpointConvention<Command>(new Uri("queue:commands"));
        });

        // Act
        var queue = provider.GetRequiredService<RabbitMqBusRegistrationContext>().GetQueueName(typeof(Command));

        // Assert
        queue.ShouldBe("commands");
    }

    [Test]
    public void GetQueueName_ConsumerWithoutEndpointHasNoImplicitSendTarget()
    {
        // Arrange
        using var provider = Build(builder => builder.UsingRabbitMq((_, rabbit) =>
            rabbit.Host("localhost", "/", _ => { })));

        // Act
        var exception = Should.Throw<InvalidOperationException>(() =>
            provider.GetRequiredService<RabbitMqBusRegistrationContext>().GetQueueName(typeof(Command)));

        // Assert
        exception.Message.ShouldContain("No queue");
    }

    [Test]
    public void GetQueueName_ProducerOnlyConventionDoesNotRequireConsumer()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddConveyo(builder =>
        {
            builder.Map<Command>("test:command.v1");
            builder.MapEndpointConvention<Command>(new Uri("queue:remote%20commands"));
            builder.UsingRabbitMq((_, rabbit) => rabbit.Host("localhost", "/", _ => { }));
        });
        using var provider = services.BuildServiceProvider();

        // Act
        var queue = provider.GetRequiredService<RabbitMqBusRegistrationContext>().GetQueueName(typeof(Command));

        // Assert
        queue.ShouldBe("remote commands");
    }

    private static ServiceProvider Build(Action<IConveyoBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddConveyo(builder =>
        {
            builder.Map<Command>("test:command.v1");
            builder.AddConsumer<Consumer>();
            configure(builder);
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
    }

    private static void ConfigureTwoQueues(IConveyoBuilder builder) => builder.UsingRabbitMq((context, rabbit) =>
    {
        rabbit.Host("localhost", "/", _ => { });
        rabbit.ReceiveEndpoint("Commands", endpoint => endpoint.ConfigureConsumer<Consumer>(context));
        rabbit.ReceiveEndpoint("commands", endpoint => endpoint.ConfigureConsumer<Consumer>(context));
    });

    private sealed record Command(string Value);
    private sealed class Consumer : IConsumer<Command>
    {
        public Task Consume(ConsumeContext<Command> context) => Task.CompletedTask;
    }
}
