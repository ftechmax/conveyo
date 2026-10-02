using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.RabbitMq;

namespace Conveyo.RabbitMQ.Test.Integration;

[TestFixture]
[Category("Integration")]
public class RabbitMqRecoveryIntegrationTests
{
    [Test]
    public async Task BrokerOutage_RejectsPublishingThenRecoversTopologyConsumerAndPublisherConfirms()
    {
        // Arrange
        await using var broker = new RabbitMqBuilder("rabbitmq:4.3.0-management")
            .WithLabel(IntegrationContainers.RunLabels).Build();
        await IntegrationContainers.StartAsync(broker);
        var queue = $"conveyo-it-recovery-{Guid.NewGuid():N}";
        var capture = new Capture();
        var options = BrokerFixture.GetOptions(broker, "recovery");
        options.NetworkRecoveryInterval = TimeSpan.FromMilliseconds(200);
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(capture);
        services.AddConveyo(builder =>
        {
            builder.Map<Event>($"test:recovery.{Guid.NewGuid():N}");
            builder.AddConsumer<Consumer>();
            builder.UsingRabbitMq((context, rabbit) =>
            {
                rabbit.HostOptions = options;
                rabbit.ReceiveEndpoint(queue, endpoint => endpoint.ConfigureConsumer<Consumer>(context));
            });
        });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().Single();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        try
        {
            await hosted.StartAsync(timeout.Token);
            var bus = (RabbitMqBusRegistrationContext)provider.GetRequiredService<IRabbitMqBusRegistrationContext>();
            var connection = bus.Connection!;
            var channel = bus.Channel!;
            var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            connection.RecoverySucceededAsync += (_, _) => { recovered.TrySetResult(); return Task.CompletedTask; };
            connection.ConnectionShutdownAsync += (_, _) => { disconnected.TrySetResult(); return Task.CompletedTask; };
            var endpoints = provider.GetRequiredService<IEndpointProvider>();
            var publish = endpoints.GetPublishEndpoint<Event>();
            var send = endpoints.GetSendEndpoint<Event>();
            await publish.Publish(new Event("before"), timeout.Token);
            var before = await capture.Deliveries.Reader.ReadAsync(timeout.Token);

            // Act
            var stop = await broker.ExecAsync(new[] { "rabbitmqctl", "stop_app" }, timeout.Token);
            stop.ExitCode.ShouldBe(0, stop.Stderr);
            await disconnected.Task.WaitAsync(timeout.Token);
            var failedPublish = await Should.ThrowAsync<Exception>(() => publish.Publish(new Event("during"), timeout.Token));
            var start = await broker.ExecAsync(new[] { "rabbitmqctl", "start_app" }, timeout.Token);
            start.ExitCode.ShouldBe(0, start.Stderr);
            await recovered.Task.WaitAsync(timeout.Token);
            await publish.Publish(new Event("after-publish"), timeout.Token);
            var afterPublish = await capture.Deliveries.Reader.ReadAsync(timeout.Token);
            await send.Send(new Event("after-send"), timeout.Token);
            var afterSend = await capture.Deliveries.Reader.ReadAsync(timeout.Token);

            // Assert
            (failedPublish is OperationCanceledException).ShouldBeFalse();
            before.ShouldBe("before");
            afterPublish.ShouldBe("after-publish");
            afterSend.ShouldBe("after-send");
            bus.Connection.ShouldBeSameAs(connection);
            bus.Channel.ShouldBeSameAs(channel);
            connection.IsOpen.ShouldBeTrue();
            channel.IsOpen.ShouldBeTrue();
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    private sealed record Event(string Value);
    private sealed class Capture
    {
        public Channel<string> Deliveries { get; } = Channel.CreateUnbounded<string>();
    }
    private sealed class Consumer(Capture capture) : IConsumer<Event>
    {
        public Task Consume(ConsumeContext<Event> context)
        {
            capture.Deliveries.Writer.TryWrite(context.Message.Value);
            return Task.CompletedTask;
        }
    }
}
