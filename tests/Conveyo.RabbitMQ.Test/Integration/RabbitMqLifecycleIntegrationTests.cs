using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Conveyo.RabbitMQ.Test.Integration;

[TestFixture]
[Category("Integration")]
public class RabbitMqLifecycleIntegrationTests
{
    [Test]
    public async Task FailedStartupTopology_DisposesConnectionAndAllowsFreshStartup()
    {
        // Arrange
        await using var setupConnection = new ConnectionScope(await BrokerFixture.StartConnectionAsync("startup-conflict"));
        await using var setup = await setupConnection.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var urn = $"test:conflict.{Guid.NewGuid():N}";
        await setup.ExchangeDeclareAsync(urn, ExchangeType.Direct, durable: true);
        var services = Services();
        services.AddConveyo(builder =>
        {
            builder.Map<Command>(urn);
            builder.UsingRabbitMq((_, rabbit) => rabbit.HostOptions = BrokerFixture.GetOptions("failed-startup"));
        });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().Single();
        var bus = (RabbitMqBusRegistrationContext)provider.GetRequiredService<IRabbitMqBusRegistrationContext>();

        // Act
        var failure = await Should.ThrowAsync<OperationInterruptedException>(() => hosted.StartAsync(CancellationToken.None));
        var connectionAfterFailure = bus.Connection;
        var channelAfterFailure = bus.Channel;
        await setup.ExchangeDeleteAsync(urn);
        try
        {
            await hosted.StartAsync(CancellationToken.None);

            // Assert
            failure.ShutdownReason!.ReplyCode.ShouldBe((ushort)406);
            connectionAfterFailure.ShouldBeNull();
            channelAfterFailure.ShouldBeNull();
            bus.Connection.ShouldNotBeNull();
            bus.Connection!.IsOpen.ShouldBeTrue();
            await setup.ExchangeDeclarePassiveAsync(urn);
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
            await setup.ExchangeDeleteAsync(urn);
        }
    }

    [Test]
    public async Task Shutdown_CancelsActiveConsumerAndRequeuesUnacknowledgedDelivery()
    {
        // Arrange
        var queue = $"conveyo-it-shutdown-{Guid.NewGuid():N}";
        var capture = new Capture();
        var services = Services();
        services.AddSingleton(capture);
        services.AddConveyo(builder =>
        {
            builder.Map<Command>($"test:shutdown.{Guid.NewGuid():N}");
            builder.AddConsumer<WaitingConsumer>();
            builder.UsingRabbitMq((context, rabbit) =>
            {
                rabbit.HostOptions = BrokerFixture.GetOptions("shutdown");
                rabbit.ReceiveEndpoint(queue, endpoint => endpoint.ConfigureConsumer<WaitingConsumer>(context));
            });
        });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().Single();
        var bus = (RabbitMqBusRegistrationContext)provider.GetRequiredService<IRabbitMqBusRegistrationContext>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await using var setupConnection = new ConnectionScope(await BrokerFixture.StartConnectionAsync("shutdown-observer"));
        await using var setup = await setupConnection.Inner.CreatePublisherChannelAsync(timeout.Token);
        try
        {
            await hosted.StartAsync(timeout.Token);
            await provider.GetRequiredService<IEndpointProvider>().GetSendEndpoint<Command>().Send(new Command("wait"), timeout.Token);
            await capture.Entered.Task.WaitAsync(timeout.Token);

            // Act
            await hosted.StopAsync(timeout.Token);
            await hosted.StopAsync(timeout.Token);
            var redelivery = await setup.BasicGetAsync(queue, autoAck: true, timeout.Token);

            // Assert
            capture.Cancelled.Task.IsCompleted.ShouldBeTrue();
            bus.Connection.ShouldBeNull();
            bus.Channel.ShouldBeNull();
            redelivery.ShouldNotBeNull();
            redelivery.Redelivered.ShouldBeTrue();
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
            await setup.QueueDeleteAsync(queue);
        }
    }

    [Test]
    public async Task CancelledShutdown_DisposesResourcesAndRepeatedStopSucceeds()
    {
        // Arrange
        var manager = await BrokerFixture.StartConnectionAsync("cancelled-stop");
        var connection = manager.Connection!;
        var channel = manager.ConsumerChannel!;
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        // Act
        await manager.StopAsync(cancellation.Token);
        await manager.StopAsync(CancellationToken.None);

        // Assert
        manager.Connection.ShouldBeNull();
        manager.ConsumerChannel.ShouldBeNull();
        connection.IsOpen.ShouldBeFalse();
        channel.IsOpen.ShouldBeFalse();
    }

    [Test]
    public async Task DeletedFaultExchange_IsRedeclaredOnTheNextPublishWithoutSubscribers()
    {
        // Arrange
        var urn = $"test:fault-redeclare.{Guid.NewGuid():N}";
        var services = Services();
        services.AddConveyo(builder =>
        {
            builder.Map<Command>(urn);
            builder.UsingRabbitMq((_, rabbit) => rabbit.HostOptions = BrokerFixture.GetOptions("fault-redeclare"));
        });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().Single();
        await using var setupConnection = new ConnectionScope(await BrokerFixture.StartConnectionAsync("fault-observer"));
        await using var setup = await setupConnection.Inner.CreatePublisherChannelAsync(CancellationToken.None);
        var fault = new Fault<Command>
        {
            FaultId = Guid.NewGuid(),
            FaultedMessageId = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow,
            Exceptions = new[] { ExceptionInfo.From(new InvalidOperationException("private details")) },
            Host = new HostInfo(),
            Message = new Command("failed")
        };
        try
        {
            await hosted.StartAsync(CancellationToken.None);
            var endpoint = provider.GetRequiredService<IEndpointProvider>().GetPublishEndpoint<Fault<Command>>();
            await endpoint.Publish(fault);
            await setup.ExchangeDeleteAsync($"{urn}.fault");

            // Act
            await endpoint.Publish(fault);

            // Assert
            // The real publisher confirms even with zero bindings; the deleted exchange exists again.
            await setup.ExchangeDeclarePassiveAsync($"{urn}.fault");
            setup.IsOpen.ShouldBeTrue();
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
            await setup.ExchangeDeleteAsync($"{urn}.fault");
            await setup.ExchangeDeleteAsync(urn);
        }
    }

    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        return services;
    }
    private sealed record Command(string Value);
    private sealed class Capture
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    private sealed class WaitingConsumer(Capture capture) : IConsumer<Command>
    {
        public async Task Consume(ConsumeContext<Command> context)
        {
            capture.Entered.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, context.CancellationToken);
            }
            catch (OperationCanceledException)
            {
                capture.Cancelled.TrySetResult();
                throw;
            }
        }
    }
}
