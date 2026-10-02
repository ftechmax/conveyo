using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Conveyo.Test;

[TestFixture]
public class ConveyoHostedServiceMessageDataTests
{
    [Test]
    public async Task HostedService_HydratesStringMessageDataFromRawUtf8()
    {
        // Arrange
        var payload = Encoding.UTF8.GetBytes("ZzzzZZZZ");

        // Act
        var capture = await Hydrate<StringPayloadMessage, StringPayloadConsumer, StringCapture>(
            "conveyo:test.string-payload.v1", payload);

        // Assert
        capture.Value.ShouldBe("ZzzzZZZZ");
    }

    [Test]
    public async Task HostedService_HydratesByteArrayMessageDataFromRawBytes()
    {
        // Arrange
        byte[] payload = [0, 1, 2, 3, 4, 5];

        // Act
        var capture = await Hydrate<BytesPayloadMessage, BytesPayloadConsumer, BytesCapture>(
            "conveyo:test.bytes-payload.v1", payload);

        // Assert
        capture.Value.ShouldBe(payload);
    }

    [Test]
    public async Task HostedService_HydratesStreamMessageDataAsReadableStream()
    {
        // Arrange
        var payload = Encoding.UTF8.GetBytes("SOEPAHSTREAmmmmm");

        // Act
        var capture = await Hydrate<StreamPayloadMessage, StreamPayloadConsumer, StreamCapture>(
            "conveyo:test.stream-payload.v1", payload);

        // Assert
        capture.Value.ShouldBe("SOEPAHSTREAmmmmm");
    }

    [Test]
    public async Task HostedService_RejectsStringMessageDataAboveConfiguredLimit()
    {
        // Arrange
        var payload = Encoding.UTF8.GetBytes("12345");

        // Act
        var ex = await Should.ThrowAsync<InvalidDataException>(() =>
            Hydrate<StringPayloadMessage, StringPayloadConsumer, StringCapture>(
                urn: "conveyo:test.string-payload.v1",
                payload: payload,
                configure: builder => builder.MaxMessageDataBytes(4)));

        // Assert
        ex.Message.ShouldContain("exceeds the configured 4 byte limit");
    }

    [Test]
    public async Task HostedService_RejectsInlineDataUriPayloadAboveConfiguredLimit()
    {
        // Arrange
        var registrationContext = new FakeBusRegistrationContext();
        var endpointProvider = new FakeEndpointProvider(repository: null!);

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IBusRegistrationContext>(registrationContext);
        services.AddSingleton<IEndpointProvider>(endpointProvider);
        services.AddSingleton<StringCapture>();
        services.AddConveyo(i =>
        {
            i.MaxMessageDataBytes(4);
            i.Map<StringPayloadMessage>("conveyo:test.inline-payload-limit.v1");
            i.AddConsumer<StringPayloadConsumer>();
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);
        try
        {
            var dataUri = "data:text/plain;base64," + Convert.ToBase64String(Encoding.UTF8.GetBytes("12345"));
            var envelope = CreateEnvelope("conveyo:test.inline-payload-limit.v1", new
            {
                payload = new { address = dataUri }
            });

            // Act
            var ex = await Should.ThrowAsync<InvalidDataException>(() => registrationContext.DeliverAsync(envelope));

            // Assert
            ex.Message.ShouldContain("exceeds the configured 4 byte limit");
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task HostedService_HydratesInlineDataUriPayloadWithoutRepository()
    {
        // Arrange
        var registrationContext = new FakeBusRegistrationContext();
        var endpointProvider = new FakeEndpointProvider(repository: null!);

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IBusRegistrationContext>(registrationContext);
        services.AddSingleton<IEndpointProvider>(endpointProvider);
        services.AddSingleton<StringCapture>();
        services.AddConveyo(i =>
        {
            i.Map<StringPayloadMessage>("conveyo:test.inline-payload.v1");
            i.AddConsumer<StringPayloadConsumer>();
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var dataUri = "data:text/plain;base64," + Convert.ToBase64String(Encoding.UTF8.GetBytes("inline-hello"));
            var envelope = CreateEnvelope("conveyo:test.inline-payload.v1", new
            {
                payload = new { address = dataUri }
            });

            // Act
            await registrationContext.DeliverAsync(envelope);

            // Assert
            serviceProvider.GetRequiredService<StringCapture>().Value.ShouldBe("inline-hello");
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task HostedService_LimitsStreamMessageDataReadByConsumer()
    {
        // Arrange
        var payload = Encoding.UTF8.GetBytes("12345");

        // Act
        var ex = await Should.ThrowAsync<InvalidDataException>(() =>
            Hydrate<StreamPayloadMessage, StreamPayloadConsumer, StreamCapture>(
                urn: "conveyo:test.stream-payload.v1",
                payload: payload,
                configure: builder => builder.MaxMessageDataBytes(4)));

        // Assert
        ex.Message.ShouldContain("exceeds the configured 4 byte limit");
    }

    private static async Task<TCapture> Hydrate<TMessage, TConsumer, TCapture>(
        string urn,
        byte[] payload,
        Action<IConveyoBuilder>? configure = null)
        where TMessage : class
        where TConsumer : class, IConsumer<TMessage>
        where TCapture : class, new()
    {
        var registrationContext = new FakeBusRegistrationContext();
        var repository = new InMemoryMessageDataRepository();
        var endpointProvider = new FakeEndpointProvider(repository);

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IBusRegistrationContext>(registrationContext);
        services.AddSingleton<IEndpointProvider>(endpointProvider);
        services.AddSingleton<IMessageDataRepository>(repository);
        services.AddSingleton<TCapture>();
        services.AddConveyo(i =>
        {
            configure?.Invoke(i);
            i.Map<TMessage>(urn);
            i.AddConsumer<TConsumer>();
        });

        await using var serviceProvider = services.BuildServiceProvider();
        var hostedService = serviceProvider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        try
        {
            var address = repository.Store(payload);
            var envelope = CreateEnvelope(urn, new
            {
                payload = new { address = address.ToString() }
            });

            await registrationContext.DeliverAsync(envelope);

            return serviceProvider.GetRequiredService<TCapture>();
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    private static MessageEnvelope CreateEnvelope(string urn, object payload)
    {
        return new MessageEnvelope
        {
            MessageType = [urn],
            Message = JsonSerializer.SerializeToElement(payload)
        };
    }

    private sealed record StringPayloadMessage
    {
        public MessageData<string> Payload { get; init; } = null!;
    }

    private sealed class StringCapture
    {
        public string? Value { get; set; }
    }

    private sealed class StringPayloadConsumer(StringCapture capture) : IConsumer<StringPayloadMessage>
    {
        public Task Consume(ConsumeContext<StringPayloadMessage> context)
        {
            capture.Value = context.Message.Payload.Value;
            return Task.CompletedTask;
        }
    }

    private sealed record BytesPayloadMessage
    {
        public MessageData<byte[]> Payload { get; init; } = null!;
    }

    private sealed class BytesCapture
    {
        public byte[]? Value { get; set; }
    }

    private sealed class BytesPayloadConsumer(BytesCapture capture) : IConsumer<BytesPayloadMessage>
    {
        public Task Consume(ConsumeContext<BytesPayloadMessage> context)
        {
            capture.Value = context.Message.Payload.Value;
            return Task.CompletedTask;
        }
    }

    private sealed record StreamPayloadMessage
    {
        public MessageData<Stream> Payload { get; init; } = null!;
    }

    private sealed class StreamCapture
    {
        public string? Value { get; set; }
    }

    private sealed class StreamPayloadConsumer(StreamCapture capture) : IConsumer<StreamPayloadMessage>
    {
        public async Task Consume(ConsumeContext<StreamPayloadMessage> context)
        {
            await using var stream = context.Message.Payload.Value;
            stream.ShouldNotBeNull();
            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: false);
            capture.Value = await reader.ReadToEndAsync(CancellationToken.None);
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

    private sealed class FakeEndpointProvider(IMessageDataRepository repository) : IEndpointProvider
    {
        public IPublishEndpoint GetPublishEndpoint<T>() where T : class => NoOpEndpoint.Instance;

        public ISendEndpoint GetSendEndpoint<T>() where T : class => NoOpEndpoint.Instance;

        public ISendEndpoint GetSendEndpoint<T>(Uri address) where T : class => NoOpEndpoint.Instance;

        public IMessageDataRepository? MessageData { get; } = repository;
    }

    private sealed class NoOpEndpoint : IPublishEndpoint, ISendEndpoint
    {
        public static NoOpEndpoint Instance { get; } = new();

        public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
            Task.CompletedTask;

        public Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class =>
            Task.CompletedTask;
    }

    private sealed class InMemoryMessageDataRepository : IMessageDataRepository
    {
        private readonly Dictionary<string, byte[]> _store = new(StringComparer.Ordinal);

        public Uri Store(byte[] payload)
        {
            var address = new Uri($"mem://payload/{Guid.NewGuid():N}");
            _store[address.AbsoluteUri] = payload;
            return address;
        }

        public async Task<Uri> PutAsync(Stream data, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default)
        {
            using var ms = new MemoryStream();
            await data.CopyToAsync(ms, cancellationToken);
            return Store(ms.ToArray());
        }

        public Task<Stream> GetAsync(Uri address, CancellationToken cancellationToken = default)
        {
            if (!_store.TryGetValue(address.AbsoluteUri, out var payload))
            {
                throw new FileNotFoundException($"MessageData not found: {address}");
            }

            return Task.FromResult<Stream>(new MemoryStream(payload, writable: false));
        }
    }
}
