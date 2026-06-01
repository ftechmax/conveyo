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
    public Task HostedService_HydratesStringMessageDataFromRawUtf8() =>
        RunHydrationTest<StringPayloadMessage, StringPayloadConsumer, StringCapture>(
            urn: "conveyo:test.string-payload.v1",
            payload: Encoding.UTF8.GetBytes("ZzzzZZZZ"),
            assertResult: capture => Assert.That(capture.Value, Is.EqualTo("ZzzzZZZZ")));

    [Test]
    public Task HostedService_HydratesByteArrayMessageDataFromRawBytes() =>
        RunHydrationTest<BytesPayloadMessage, BytesPayloadConsumer, BytesCapture>(
            urn: "conveyo:test.bytes-payload.v1",
            payload: [0, 1, 2, 3, 4, 5],
            assertResult: capture => Assert.That(capture.Value, Is.EqualTo(new byte[] { 0, 1, 2, 3, 4, 5 })));

    [Test]
    public Task HostedService_HydratesStreamMessageDataAsReadableStream() =>
        RunHydrationTest<StreamPayloadMessage, StreamPayloadConsumer, StreamCapture>(
            urn: "conveyo:test.stream-payload.v1",
            payload: Encoding.UTF8.GetBytes("SOEPAHSTREAmmmmm"),
            assertResult: capture => Assert.That(capture.Value, Is.EqualTo("SOEPAHSTREAmmmmm")));

    [Test]
    public void HostedService_RejectsStringMessageDataAboveConfiguredLimit()
    {
        var ex = Assert.ThrowsAsync<InvalidDataException>(() =>
            RunHydrationTest<StringPayloadMessage, StringPayloadConsumer, StringCapture>(
                urn: "conveyo:test.string-payload.v1",
                payload: Encoding.UTF8.GetBytes("12345"),
                assertResult: _ => throw new AssertionException("Oversized MessageData should not reach the consumer."),
                configure: builder => builder.MaxMessageDataBytes(4)));

        Assert.That(ex!.Message, Does.Contain("exceeds the configured 4 byte limit"));
    }

    [Test]
    public void HostedService_RejectsInlineDataUriPayloadAboveConfiguredLimit()
    {
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

        var ex = Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await using var serviceProvider = services.BuildServiceProvider();
            var hostedService = serviceProvider.GetServices<IHostedService>().Single();
            await hostedService.StartAsync(CancellationToken.None);
            try
            {
                var dataUri = "data:text/plain;base64," + Convert.ToBase64String(Encoding.UTF8.GetBytes("12345"));
                var envelope = CreateEnvelope("conveyo:test.inline-payload-limit.v1", new
                {
                    weatherData = new { address = dataUri }
                });
                await registrationContext.DeliverAsync(envelope);
            }
            finally
            {
                await hostedService.StopAsync(CancellationToken.None);
            }
        });

        Assert.That(ex!.Message, Does.Contain("exceeds the configured 4 byte limit"));
    }

    [Test]
    public async Task HostedService_HydratesInlineDataUriPayloadWithoutRepository()
    {
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
                weatherData = new { address = dataUri }
            });

            await registrationContext.DeliverAsync(envelope);

            Assert.That(serviceProvider.GetRequiredService<StringCapture>().Value, Is.EqualTo("inline-hello"));
        }
        finally
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public void HostedService_LimitsStreamMessageDataReadByConsumer()
    {
        var ex = Assert.ThrowsAsync<InvalidDataException>(() =>
            RunHydrationTest<StreamPayloadMessage, StreamPayloadConsumer, StreamCapture>(
                urn: "conveyo:test.stream-payload.v1",
                payload: Encoding.UTF8.GetBytes("12345"),
                assertResult: _ => throw new AssertionException("Oversized MessageData should not be fully read."),
                configure: builder => builder.MaxMessageDataBytes(4)));

        Assert.That(ex!.Message, Does.Contain("exceeds the configured 4 byte limit"));
    }

    private static async Task RunHydrationTest<TMessage, TConsumer, TCapture>(
        string urn,
        byte[] payload,
        Action<TCapture> assertResult,
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
                weatherData = new { address = address.ToString() }
            });

            await registrationContext.DeliverAsync(envelope);

            assertResult(serviceProvider.GetRequiredService<TCapture>());
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
        public MessageData<string> WeatherData { get; init; } = null!;
    }

    private sealed class StringCapture
    {
        public string? Value { get; set; }
    }

    private sealed class StringPayloadConsumer(StringCapture capture) : IConsumer<StringPayloadMessage>
    {
        public Task Consume(ConsumeContext<StringPayloadMessage> context)
        {
            capture.Value = context.Message.WeatherData.Value;
            return Task.CompletedTask;
        }
    }

    private sealed record BytesPayloadMessage
    {
        public MessageData<byte[]> WeatherData { get; init; } = null!;
    }

    private sealed class BytesCapture
    {
        public byte[]? Value { get; set; }
    }

    private sealed class BytesPayloadConsumer(BytesCapture capture) : IConsumer<BytesPayloadMessage>
    {
        public Task Consume(ConsumeContext<BytesPayloadMessage> context)
        {
            capture.Value = context.Message.WeatherData.Value;
            return Task.CompletedTask;
        }
    }

    private sealed record StreamPayloadMessage
    {
        public MessageData<Stream> WeatherData { get; init; } = null!;
    }

    private sealed class StreamCapture
    {
        public string? Value { get; set; }
    }

    private sealed class StreamPayloadConsumer(StreamCapture capture) : IConsumer<StreamPayloadMessage>
    {
        public async Task Consume(ConsumeContext<StreamPayloadMessage> context)
        {
            await using var stream = context.Message.WeatherData.Value;
            Assert.That(stream, Is.Not.Null);
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
