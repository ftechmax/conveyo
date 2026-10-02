using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Conveyo.Test;

[TestFixture]
public class ConveyoHostedServiceDispatchTests
{
    [Test]
    public async Task HostedService_DispatchesOnlyConsumersRegisteredAtDestinationQueue()
    {
        // Arrange
        var registrationContext = new FakeBusRegistrationContext();
        var serviceProvider = BuildServiceProvider(registrationContext, builder =>
        {
            builder.Services.AddSingleton<FirstCapture>();
            builder.Services.AddSingleton<SecondCapture>();
            builder.Conveyo.Map<SharedMessage>("conveyo:test.shared.v1");
            builder.Conveyo.AddConsumer<FirstSharedMessageConsumer>();
            builder.Conveyo.AddConsumer<SecondSharedMessageConsumer>();
            builder.ReceiveEndpoint<FirstSharedMessageConsumer>(new Uri("queue:first"));
            builder.ReceiveEndpoint<SecondSharedMessageConsumer>(new Uri("queue:second"));
        });
        await using var _ = serviceProvider;

        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            // Act
            await registrationContext.DeliverAsync(CreateEnvelope<SharedMessage>(
                new { Value = "hello" },
                new Uri("queue:second")));

            // Assert
            serviceProvider.GetRequiredService<FirstCapture>().Value.ShouldBeNull();
            serviceProvider.GetRequiredService<SecondCapture>().Value.ShouldBe("hello");
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task HostedService_DispatchesAllConsumersRegisteredAtDestinationQueue()
    {
        // Arrange
        var registrationContext = new FakeBusRegistrationContext();
        var serviceProvider = BuildServiceProvider(registrationContext, builder =>
        {
            builder.Services.AddSingleton<FirstCapture>();
            builder.Services.AddSingleton<SecondCapture>();
            builder.Conveyo.Map<SharedMessage>("conveyo:test.shared.v1");
            builder.Conveyo.AddConsumer<FirstSharedMessageConsumer>();
            builder.Conveyo.AddConsumer<SecondSharedMessageConsumer>();
            builder.ReceiveEndpoint<FirstSharedMessageConsumer>(new Uri("queue:shared"));
            builder.ReceiveEndpoint<SecondSharedMessageConsumer>(new Uri("queue:shared"));
        });
        await using var _ = serviceProvider;

        var destinationAddress = new Uri("queue:shared");

        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            // Act
            await registrationContext.DeliverAsync(CreateEnvelope<SharedMessage>(
                new { Value = "fanout" },
                destinationAddress));

            // Assert
            serviceProvider.GetRequiredService<FirstCapture>().Value.ShouldBe("fanout");
            serviceProvider.GetRequiredService<SecondCapture>().Value.ShouldBe("fanout");
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task HostedService_ResolvesByBaseUrn_WhenPrimaryUrnIsUnknown()
    {
        // Arrange
        var registrationContext = new FakeBusRegistrationContext();
        var serviceProvider = BuildServiceProvider(registrationContext, builder =>
        {
            builder.Services.AddSingleton<BaseCapture>();
            builder.Conveyo.Map<OrderCreatedBase>("conveyo:orders.order-created");
            builder.Conveyo.AddConsumer<BaseOrderCreatedConsumer>();
            builder.ReceiveEndpoint<BaseOrderCreatedConsumer>(new Uri("queue:orders"));
        });
        await using var _ = serviceProvider;

        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var envelope = new MessageEnvelope
            {
                DestinationAddress = new Uri("queue:orders"),
                MessageType =
                [
                    "conveyo:orders.order-created.v2",
                    "conveyo:orders.order-created"
                ],
                Message = JsonSerializer.SerializeToElement(new { Id = "abc" })
            };

            // Act
            await registrationContext.DeliverAsync(envelope);

            // Assert
            serviceProvider.GetRequiredService<BaseCapture>().Id.ShouldBe("abc");
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task HostedService_PublishesFaultAfterRetriesExhausted()
    {
        // Arrange
        var registrationContext = new FakeBusRegistrationContext();
        var publishCapture = new CapturingEndpointProvider();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IBusRegistrationContext>(registrationContext);
        services.AddSingleton<IEndpointProvider>(publishCapture);
        services.AddSingleton<FirstCapture>();
        services.AddConveyo(conveyo =>
        {
            conveyo.Map<SharedMessage>("conveyo:test.shared.v1");
            conveyo.AddConsumer<FirstSharedMessageConsumer>();
        });

        await using var serviceProvider = services.BuildServiceProvider();

        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var failedMessageId = Guid.NewGuid();
            var envelope = new MessageEnvelope
            {
                MessageId = failedMessageId,
                MessageType = ["conveyo:test.shared.v1"],
                Message = JsonSerializer.SerializeToElement(new { Value = "boom" })
            };

            var first = new InvalidOperationException("first attempt failed");
            var second = new InvalidOperationException("second attempt failed", new ArgumentException("inner cause"));

            // Act
            await registrationContext.RaiseFaultAsync(envelope, [first, second]);

            // Assert
            var fault = publishCapture.Published.OfType<Fault<SharedMessage>>().Single();
            serviceProvider.GetRequiredService<ConveyoContext>().UrnFor(typeof(Fault<SharedMessage>)).ShouldBe("conveyo:test.shared.v1.fault");
            fault.FaultedMessageId.ShouldBe(failedMessageId);
            fault.Message.Value.ShouldBe("boom");
            fault.Exceptions.Length.ShouldBe(2);
            fault.Exceptions[0].ExceptionType.ShouldBe("System.InvalidOperationException");
            fault.Exceptions[0].Message.ShouldBe(ExceptionInfo.RedactedMessage);
            fault.Exceptions[0].StackTrace.ShouldBeNull();
            fault.Exceptions[1].ExceptionType.ShouldBe("System.InvalidOperationException");
            fault.Exceptions[1].Message.ShouldBe(ExceptionInfo.RedactedMessage);
            fault.Exceptions[1].StackTrace.ShouldBeNull();
            fault.Exceptions[1].InnerException.ShouldBeNull();
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task HostedService_IncludesFaultExceptionDetailsWhenConfigured()
    {
        // Arrange
        var registrationContext = new FakeBusRegistrationContext();
        var publishCapture = new CapturingEndpointProvider();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IBusRegistrationContext>(registrationContext);
        services.AddSingleton<IEndpointProvider>(publishCapture);
        services.AddSingleton<FirstCapture>();
        services.AddConveyo(conveyo =>
        {
            conveyo.IncludeFaultExceptionDetails();
            conveyo.Map<SharedMessage>("conveyo:test.shared.v1");
            conveyo.AddConsumer<FirstSharedMessageConsumer>();
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var envelope = new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                MessageType = ["conveyo:test.shared.v1"],
                Message = JsonSerializer.SerializeToElement(new { Value = "boom" })
            };

            var exception = new InvalidOperationException("outer message", new ArgumentException("inner message"));

            // Act
            await registrationContext.RaiseFaultAsync(envelope, [exception]);

            // Assert
            var faultException = publishCapture.Published.OfType<Fault<SharedMessage>>().Single().Exceptions.Single();
            faultException.Message.ShouldBe("outer message");
            faultException.InnerException.ShouldNotBeNull();
            faultException.InnerException!.Message.ShouldBe("inner message");
            faultException.InnerException.ExceptionType.ShouldBe("System.ArgumentException");
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task HostedService_PublishesFaultOnConsumerFailure()
    {
        // Arrange
        var registrationContext = new FakeBusRegistrationContext();
        var capture = new CapturingEndpointProvider();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IBusRegistrationContext>(registrationContext);
        services.AddSingleton<IEndpointProvider>(capture);
        services.AddSingleton<FirstCapture>();
        services.AddConveyo(conveyo =>
        {
            conveyo.Map<SharedMessage>("conveyo:test.shared.v1");
            conveyo.AddConsumer<FirstSharedMessageConsumer>();
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var envelope = new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                MessageType = ["conveyo:test.shared.v1"],
                Message = JsonSerializer.SerializeToElement(new { Value = "boom" })
            };

            // Act
            await registrationContext.RaiseFaultAsync(envelope, [new InvalidOperationException("boom")]);

            // Assert
            capture.Sent.ShouldBeEmpty();
            capture.Published.OfType<Fault<SharedMessage>>().Count().ShouldBe(1);
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    // Envelope-version validation now lives in the transport's EnvelopeSerializer (the wire layer),
    // not the hosted service. The transport routes mismatched envelopes to the _error queue with
    // conveyo-fault-reason = "deserialization-failed" before they ever reach OnMessageAsync.
    // See: Conveyo.RabbitMQ.Test EnvelopeSerializerTests for that contract.

    private static ServiceProvider BuildServiceProvider(
        FakeBusRegistrationContext registrationContext,
        Action<TestBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IBusRegistrationContext>(registrationContext);
        services.AddSingleton<IEndpointProvider>(new FakeEndpointProvider());

        services.AddConveyo(conveyo =>
        {
            configure(new TestBuilder(services, conveyo));
        });

        return services.BuildServiceProvider();
    }

    private sealed record TestBuilder(IServiceCollection Services, IConveyoBuilder Conveyo)
    {
        public void ReceiveEndpoint<TConsumer>(Uri address) where TConsumer : class =>
            Conveyo.Registration.RegisterConsumerEndpoint(typeof(TConsumer), address);
    }

    private static MessageEnvelope CreateEnvelope<TMessage>(object payload, Uri destinationAddress)
        where TMessage : class
    {
        return new MessageEnvelope
        {
            DestinationAddress = destinationAddress,
            MessageType = ["conveyo:test.shared.v1"],
            Message = JsonSerializer.SerializeToElement(payload)
        };
    }

    private sealed record SharedMessage(string Value);

    private sealed record OrderCreatedBase(string Id);

    private sealed class BaseCapture
    {
        public string? Id { get; set; }
    }

    private sealed class BaseOrderCreatedConsumer(BaseCapture capture) : IConsumer<OrderCreatedBase>
    {
        public Task Consume(ConsumeContext<OrderCreatedBase> context)
        {
            capture.Id = context.Message.Id;
            return Task.CompletedTask;
        }
    }

    private sealed class FirstCapture
    {
        public string? Value { get; set; }
    }

    private sealed class SecondCapture
    {
        public string? Value { get; set; }
    }

    private sealed class FirstSharedMessageConsumer(FirstCapture capture) : IConsumer<SharedMessage>
    {
        public Task Consume(ConsumeContext<SharedMessage> context)
        {
            capture.Value = context.Message.Value;
            return Task.CompletedTask;
        }
    }

    private sealed class SecondSharedMessageConsumer(SecondCapture capture) : IConsumer<SharedMessage>
    {
        public Task Consume(ConsumeContext<SharedMessage> context)
        {
            capture.Value = context.Message.Value;
            return Task.CompletedTask;
        }
    }

    private sealed class FakeBusRegistrationContext : IBusRegistrationContext
    {
        public event Func<MessageEnvelope, CancellationToken, Task>? OnMessageAsync;

        public event Func<MessageEnvelope, IReadOnlyList<Exception>, CancellationToken, Task>? OnFaultAsync;

        public Task StartAsync(ConveyoContext context, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task DeliverAsync(MessageEnvelope envelope, CancellationToken cancellationToken = default) =>
            OnMessageAsync?.Invoke(envelope, cancellationToken) ?? Task.CompletedTask;

        public Task RaiseFaultAsync(MessageEnvelope envelope, IReadOnlyList<Exception> exceptions, CancellationToken cancellationToken = default) =>
            OnFaultAsync?.Invoke(envelope, exceptions, cancellationToken) ?? Task.CompletedTask;
    }

    private sealed class FakeEndpointProvider : IEndpointProvider
    {
        public IPublishEndpoint GetPublishEndpoint<T>() where T : class => NoOpEndpoint.Instance;

        public ISendEndpoint GetSendEndpoint<T>() where T : class => NoOpEndpoint.Instance;

        public ISendEndpoint GetSendEndpoint<T>(Uri address) where T : class => NoOpEndpoint.Instance;

        public IMessageDataRepository? MessageData => null;
    }

    private sealed class NoOpEndpoint : IPublishEndpoint, ISendEndpoint
    {
        public static NoOpEndpoint Instance { get; } = new();

        public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
            Task.CompletedTask;

        public Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class =>
            Task.CompletedTask;
    }

    private sealed class CapturingEndpointProvider : IEndpointProvider
    {
        public List<object> Published { get; } = [];

        public List<(Uri Address, object Message)> Sent { get; } = [];

        public IPublishEndpoint GetPublishEndpoint<T>() where T : class =>
            new CapturingPublishEndpoint(Published);

        public ISendEndpoint GetSendEndpoint<T>() where T : class => NoOpEndpoint.Instance;

        public ISendEndpoint GetSendEndpoint<T>(Uri address) where T : class =>
            new CapturingSendEndpoint(address, Sent);

        public IMessageDataRepository? MessageData => null;

        private sealed class CapturingPublishEndpoint(List<object> sink) : IPublishEndpoint
        {
            public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class
            {
                sink.Add(message);
                return Task.CompletedTask;
            }
        }

        private sealed class CapturingSendEndpoint(Uri address, List<(Uri Address, object Message)> sink) : ISendEndpoint
        {
            public Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class
            {
                sink.Add((address, message));
                return Task.CompletedTask;
            }
        }
    }
}
