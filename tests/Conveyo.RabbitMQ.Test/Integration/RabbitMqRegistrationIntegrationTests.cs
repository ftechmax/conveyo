using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Conveyo.RabbitMQ.Test.Integration;

[TestFixture]
[Category("Integration")]
public class RabbitMqRegistrationIntegrationTests
{
    [Test]
    public async Task RegisteredConsumers_ReceiveCommandsOnlyAtTheirConfiguredQueues()
    {
        // Arrange
        var id = Guid.NewGuid().ToString("N");
        var firstQueue = $"conveyo-it-first-{id}";
        var secondQueue = $"conveyo-it-second-{id}";
        var capture = new Capture();
        var services = Services();
        services.AddSingleton(capture);
        services.AddConveyo(builder =>
        {
            builder.UsingRabbitMq((context, rabbit) =>
            {
                rabbit.HostOptions = BrokerFixture.GetOptions("registered-consumers");
                rabbit.ReceiveEndpoint(firstQueue, endpoint => endpoint.ConfigureConsumer<FirstConsumer>(context));
                rabbit.ReceiveEndpoint(secondQueue, endpoint => endpoint.ConfigureConsumer<SecondConsumer>(context));
            });
            builder.Map<Command>($"test:registered.{id}");
            builder.AddConsumer<FirstConsumer>();
            builder.AddConsumer<SecondConsumer>();
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        var hosted = provider.GetServices<IHostedService>().Single();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            await hosted.StartAsync(timeout.Token);
            var endpoints = provider.GetRequiredService<IEndpointProvider>();

            // Act
            await endpoints.GetSendEndpoint<Command>(QueueAddress.Create(firstQueue)).Send(new Command("first"), timeout.Token);
            await endpoints.GetSendEndpoint<Command>(QueueAddress.Create(secondQueue)).Send(new Command("second"), timeout.Token);
            var first = await capture.First.Task.WaitAsync(timeout.Token);
            var second = await capture.Second.Task.WaitAsync(timeout.Token);

            // Assert
            first.ShouldBe("first");
            second.ShouldBe("second");
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task StartAsync_UsesSnapshotOfConfiguredHostOptions()
    {
        // Arrange
        var options = BrokerFixture.GetOptions("host-snapshot");
        var services = Services();
        services.AddConveyo(builder => builder.UsingRabbitMq((_, rabbit) => rabbit.HostOptions = options));
        options.Port = 0;
        options.Username = "changed-after-registration";
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        var hosted = provider.GetServices<IHostedService>().Single();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            // Act
            await hosted.StartAsync(timeout.Token);

            // Assert
            provider.GetRequiredService<IRabbitMqBusRegistrationContext>().Connection!.IsOpen.ShouldBeTrue();
        }
        finally
        {
            await hosted.StopAsync(CancellationToken.None);
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
        public TaskCompletionSource<string> First { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<string> Second { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    private sealed class FirstConsumer(Capture capture) : IConsumer<Command>
    {
        public Task Consume(ConsumeContext<Command> context)
        {
            capture.First.TrySetResult(context.Message.Value);
            return Task.CompletedTask;
        }
    }
    private sealed class SecondConsumer(Capture capture) : IConsumer<Command>
    {
        public Task Consume(ConsumeContext<Command> context)
        {
            capture.Second.TrySetResult(context.Message.Value);
            return Task.CompletedTask;
        }
    }
}
