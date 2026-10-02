using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Hosting;

namespace Conveyo.Test;

[TestFixture]
public class MessageExecutionTests
{
    [Test]
    public async Task Dispatch_UsesAndDisposesNewScopeForEachDelivery()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<Consumer>(capture);
        var dispatcher = provider.GetRequiredService<MessageDispatcher>();

        // Act
        await dispatcher.DispatchAsync(Envelope(), CancellationToken.None);
        await dispatcher.DispatchAsync(Envelope(), CancellationToken.None);

        // Assert
        capture.Dependencies.Count.ShouldBe(2);
        capture.Dependencies[0].ShouldNotBeSameAs(capture.Dependencies[1]);
        capture.Dependencies.ShouldAllBe(dependency => dependency.Disposed);
    }

    [Test]
    public async Task Dispatch_ConsumersForOneDeliveryShareScope()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<Consumer>(capture, builder => builder.AddConsumer<SecondConsumer>());

        // Act
        await provider.GetRequiredService<MessageDispatcher>().DispatchAsync(Envelope(), CancellationToken.None);

        // Assert
        capture.Dependencies.Count.ShouldBe(2);
        capture.Dependencies[0].ShouldBeSameAs(capture.Dependencies[1]);
        capture.Dependencies[0].Disposed.ShouldBeTrue();
    }

    [Test]
    public async Task Dispatch_DisposesScopeWhenConsumerThrows()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<FailingConsumer>(capture);

        // Act
        var exception = await Should.ThrowAsync<InvalidOperationException>(() =>
            provider.GetRequiredService<MessageDispatcher>().DispatchAsync(Envelope(), CancellationToken.None));

        // Assert
        exception.Message.ShouldBe("consumer failed");
        capture.Dependencies.Single().Disposed.ShouldBeTrue();
    }

    [Test]
    public async Task Dispatch_PropagatesCancellationAndDisposesScope()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<CancelledConsumer>(capture);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        // Act
        var exception = await Should.ThrowAsync<OperationCanceledException>(() =>
            provider.GetRequiredService<MessageDispatcher>().DispatchAsync(Envelope(), cancellation.Token));

        // Assert
        exception.CancellationToken.ShouldBe(cancellation.Token);
        capture.Dependencies.Single().Disposed.ShouldBeTrue();
    }

    [Test]
    public async Task Dispatch_ConsumerReceivesEnvelopeMetadataAndToken()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<Consumer>(capture);
        using var cancellation = new CancellationTokenSource();
        var envelope = Envelope() with
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            DestinationAddress = new Uri("queue:commands"),
            SentTime = DateTime.UtcNow,
            Host = new HostInfo { MachineName = "sender" },
            Headers = new Dictionary<string, string> { ["source"] = "test" }
        };

        // Act
        await provider.GetRequiredService<MessageDispatcher>().DispatchAsync(envelope, cancellation.Token);

        // Assert
        var context = capture.Contexts.Single();
        context.Message.Value.ShouldBe("hello");
        context.MessageId.ShouldBe(envelope.MessageId);
        context.CorrelationId.ShouldBe(envelope.CorrelationId);
        context.DestinationAddress.ShouldBe(envelope.DestinationAddress);
        context.SentTime.ShouldBe(envelope.SentTime);
        context.Host.ShouldBe(envelope.Host);
        context.Headers.ShouldBe(envelope.Headers);
        context.CancellationToken.ShouldBe(cancellation.Token);
    }

    [Test]
    public async Task Dispatch_OutboundMessagesInheritMetadataAndRestoreAmbientContext()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<Consumer>(capture);
        var envelope = Envelope() with
        {
            CorrelationId = Guid.NewGuid(),
            Headers = new Dictionary<string, string> { ["source"] = "inbound" }
        };
        var previous = new OutboundMetadata(Guid.NewGuid(), new Dictionary<string, string> { ["source"] = "previous" });
        using var ambient = OutboundContext.Push(previous);

        // Act
        await provider.GetRequiredService<MessageDispatcher>().DispatchAsync(envelope, CancellationToken.None);

        // Assert
        capture.Outbound.Count.ShouldBe(2);
        capture.Outbound.ShouldAllBe(metadata => metadata!.CorrelationId == envelope.CorrelationId);
        capture.Outbound.ShouldAllBe(metadata => metadata!.Headers!["source"] == "inbound");
        OutboundContext.Current.ShouldBeSameAs(previous);
        capture.Endpoints.Single().Disposed.ShouldBeTrue();
    }

    [Test]
    public async Task Dispatch_UnknownUrnIsNotConsumed()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<Consumer>(capture);
        var envelope = Envelope() with { MessageType = ["test:unknown.v1"] };

        // Act
        var exception = await Should.ThrowAsync<MessageNotConsumedException>(() =>
            provider.GetRequiredService<MessageDispatcher>().DispatchAsync(envelope, CancellationToken.None));

        // Assert
        exception.Message.ShouldContain("test:unknown.v1");
        capture.Contexts.ShouldBeEmpty();
    }

    [Test]
    public async Task Dispatch_UnmatchedDestinationDoesNotInvokeConsumer()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<Consumer>(capture, builder =>
            builder.Registration.RegisterConsumerEndpoint(typeof(Consumer), new Uri("queue:Commands")));
        var envelope = Envelope() with { DestinationAddress = new Uri("queue:commands") };

        // Act
        var exception = await Should.ThrowAsync<MessageNotConsumedException>(() =>
            provider.GetRequiredService<MessageDispatcher>().DispatchAsync(envelope, CancellationToken.None));

        // Assert
        exception.Message.ShouldContain("queue:commands");
        capture.Contexts.ShouldBeEmpty();
    }

    [Test]
    public async Task PublishFault_UsesScopedEndpointAndPropagatesMetadataAndToken()
    {
        // Arrange
        var capture = new Capture();
        await using var provider = Build<Consumer>(capture);
        var envelope = Envelope() with
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Headers = new Dictionary<string, string> { ["source"] = "failed" }
        };
        using var cancellation = new CancellationTokenSource();

        // Act
        await provider.GetRequiredService<MessageDispatcher>().PublishFaultAsync(
            envelope, [new InvalidOperationException("failed")], cancellation.Token);

        // Assert
        capture.Published.Single().ShouldBeOfType<Fault<Command>>().FaultedMessageId.ShouldBe(envelope.MessageId);
        capture.Outbound.Single()!.CorrelationId.ShouldBe(envelope.CorrelationId);
        capture.Outbound.Single()!.Headers.ShouldBe(envelope.Headers);
        capture.PublishToken.ShouldBe(cancellation.Token);
        capture.Endpoints.Single().Disposed.ShouldBeTrue();
        OutboundContext.Current.ShouldBeNull();
    }

    [Test]
    public async Task PublishFault_FailurePropagatesAndDisposesScope()
    {
        // Arrange
        var capture = new Capture { PublishException = new InvalidOperationException("publish failed") };
        await using var provider = Build<Consumer>(capture);

        // Act
        var exception = await Should.ThrowAsync<InvalidOperationException>(() =>
            provider.GetRequiredService<MessageDispatcher>().PublishFaultAsync(
                Envelope(), [new Exception("failed")], CancellationToken.None));

        // Assert
        exception.ShouldBeSameAs(capture.PublishException);
        capture.Endpoints.Single().Disposed.ShouldBeTrue();
        OutboundContext.Current.ShouldBeNull();
    }

    [Test]
    public async Task Start_FailureRemovesExecutionSubscriptions()
    {
        // Arrange
        var capture = new Capture();
        var transport = new Transport { StartException = new InvalidOperationException("start failed") };
        await using var provider = Build<Consumer>(capture, transport: transport);
        var hosted = provider.GetServices<IHostedService>().Single();

        // Act
        var exception = await Should.ThrowAsync<InvalidOperationException>(() => hosted.StartAsync(CancellationToken.None));

        // Assert
        exception.ShouldBeSameAs(transport.StartException);
        transport.MessageSubscribers.ShouldBe(0);
        transport.FaultSubscribers.ShouldBe(0);
    }

    [Test]
    public async Task Stop_FailureRemovesExecutionSubscriptions()
    {
        // Arrange
        var capture = new Capture();
        var transport = new Transport { StopException = new InvalidOperationException("stop failed") };
        await using var provider = Build<Consumer>(capture, transport: transport);
        var hosted = provider.GetServices<IHostedService>().Single();
        await hosted.StartAsync(CancellationToken.None);

        // Act
        var exception = await Should.ThrowAsync<InvalidOperationException>(() => hosted.StopAsync(CancellationToken.None));

        // Assert
        exception.ShouldBeSameAs(transport.StopException);
        transport.MessageSubscribers.ShouldBe(0);
        transport.FaultSubscribers.ShouldBe(0);
    }

    [Test]
    public async Task Lifecycle_TransportReceivesRuntimeSnapshotAndCancellationTokens()
    {
        // Arrange
        var capture = new Capture();
        var transport = new Transport();
        await using var provider = Build<Consumer>(capture, transport: transport);
        var hosted = provider.GetServices<IHostedService>().Single();
        using var start = new CancellationTokenSource();
        using var stop = new CancellationTokenSource();

        // Act
        await hosted.StartAsync(start.Token);
        await hosted.StopAsync(stop.Token);

        // Assert
        transport.Context.ShouldBeSameAs(provider.GetRequiredService<ConveyoContext>());
        transport.StartToken.ShouldBe(start.Token);
        transport.StopToken.ShouldBe(stop.Token);
        transport.MessageSubscribers.ShouldBe(0);
        transport.FaultSubscribers.ShouldBe(0);
    }

    private static ServiceProvider Build<TConsumer>(Capture capture,
        Action<IConveyoBuilder>? configure = null, Transport? transport = null)
        where TConsumer : class, IConsumer<Command>
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(capture);
        services.AddScoped<Dependency>();
        services.AddScoped<IEndpointProvider, EndpointProvider>();
        services.AddSingleton<IBusRegistrationContext>(transport ?? new Transport());
        services.AddConveyo(builder =>
        {
            builder.Map<Command>("test:command.v1");
            builder.AddConsumer<TConsumer>();
            configure?.Invoke(builder);
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static MessageEnvelope Envelope() => new()
    {
        MessageType = ["test:command.v1"],
        Message = JsonSerializer.SerializeToElement(new Command("hello"))
    };

    private sealed record Command(string Value);
    private sealed class Capture
    {
        public List<Dependency> Dependencies { get; } = [];
        public List<ConsumeContext<Command>> Contexts { get; } = [];
        public List<EndpointProvider> Endpoints { get; } = [];
        public List<OutboundMetadata?> Outbound { get; } = [];
        public List<object> Published { get; } = [];
        public CancellationToken PublishToken { get; set; }
        public Exception? PublishException { get; init; }
    }

    private sealed class Dependency : IAsyncDisposable
    {
        public bool Disposed { get; private set; }
        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class Consumer(Capture capture, Dependency dependency) : IConsumer<Command>
    {
        public async Task Consume(ConsumeContext<Command> context)
        {
            capture.Dependencies.Add(dependency);
            capture.Contexts.Add(context);
            await context.Publish(new Command("published"));
            await context.Send(new Command("sent"));
        }
    }

    private sealed class SecondConsumer(Capture capture, Dependency dependency) : IConsumer<Command>
    {
        public Task Consume(ConsumeContext<Command> context)
        {
            capture.Dependencies.Add(dependency);
            return Task.CompletedTask;
        }
    }

    private sealed class FailingConsumer(Capture capture, Dependency dependency) : IConsumer<Command>
    {
        public Task Consume(ConsumeContext<Command> context)
        {
            capture.Dependencies.Add(dependency);
            throw new InvalidOperationException("consumer failed");
        }
    }

    private sealed class CancelledConsumer(Capture capture, Dependency dependency) : IConsumer<Command>
    {
        public Task Consume(ConsumeContext<Command> context)
        {
            capture.Dependencies.Add(dependency);
            context.CancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }
    }

    private sealed class EndpointProvider : IEndpointProvider, IPublishEndpoint, ISendEndpoint, IAsyncDisposable
    {
        private readonly Capture _capture;
        public bool Disposed { get; private set; }
        public EndpointProvider(Capture capture)
        {
            _capture = capture;
            capture.Endpoints.Add(this);
        }
        public IPublishEndpoint GetPublishEndpoint<T>() where T : class => this;
        public ISendEndpoint GetSendEndpoint<T>() where T : class => this;
        public ISendEndpoint GetSendEndpoint<T>(Uri address) where T : class => this;
        public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class
        {
            _capture.Published.Add(message);
            _capture.Outbound.Add(OutboundContext.Current);
            _capture.PublishToken = cancellationToken;
            return _capture.PublishException is { } exception ? Task.FromException(exception) : Task.CompletedTask;
        }
        public Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class
        {
            _capture.Outbound.Add(OutboundContext.Current);
            return Task.CompletedTask;
        }
        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class Transport : IBusRegistrationContext
    {
        public event Func<MessageEnvelope, CancellationToken, Task>? OnMessageAsync;
        public event Func<MessageEnvelope, IReadOnlyList<Exception>, CancellationToken, Task>? OnFaultAsync;
        public int MessageSubscribers => OnMessageAsync?.GetInvocationList().Length ?? 0;
        public int FaultSubscribers => OnFaultAsync?.GetInvocationList().Length ?? 0;
        public Exception? StartException { get; init; }
        public Exception? StopException { get; init; }
        public ConveyoContext? Context { get; private set; }
        public CancellationToken StartToken { get; private set; }
        public CancellationToken StopToken { get; private set; }
        public Task StartAsync(ConveyoContext context, CancellationToken cancellationToken)
        {
            Context = context;
            StartToken = cancellationToken;
            return StartException is { } exception ? Task.FromException(exception) : Task.CompletedTask;
        }
        public Task StopAsync(CancellationToken cancellationToken)
        {
            StopToken = cancellationToken;
            return StopException is { } exception ? Task.FromException(exception) : Task.CompletedTask;
        }
    }
}
