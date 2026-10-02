using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Conveyo.Test;

[TestFixture]
public class MessageDataHydratorTests
{
    private static readonly Uri Address = new("mem://payload/first");
    private static readonly Uri SecondAddress = new("mem://payload/second");

    [Test]
    public async Task Hydrate_DeserializesJsonReferencePayloadAndDisposesSource()
    {
        // Arrange
        var repository = new Repository();
        var stream = repository.Store(Address, "{\"value\":\"nested\"}");
        await using var provider = Build<JsonMessage>(repository);
        var message = new JsonMessage { Payload = new MessageData<JsonPayload>(Address) };

        // Act
        await Hydrate(provider, message);

        // Assert
        message.Payload.HasValue.ShouldBeTrue();
        message.Payload.Value!.Value.ShouldBe("nested");
        message.Payload.Address.ShouldBe(Address);
        stream.DisposalCount.ShouldBe(1);
    }

    [Test]
    public async Task Hydrate_JsonNullLeavesReferenceUnhydratedAndDisposesSource()
    {
        // Arrange
        var repository = new Repository();
        var stream = repository.Store(Address, "null");
        await using var provider = Build<JsonMessage>(repository);
        var message = new JsonMessage { Payload = new MessageData<JsonPayload>(Address) };

        // Act
        await Hydrate(provider, message);

        // Assert
        message.Payload.HasValue.ShouldBeFalse();
        stream.DisposalCount.ShouldBe(1);
    }

    [Test]
    public async Task Hydrate_NullPropertyDoesNotFetchPayload()
    {
        // Arrange
        var repository = new Repository();
        await using var provider = Build<JsonMessage>(repository);
        var message = new JsonMessage();

        // Act
        await Hydrate(provider, message);

        // Assert
        repository.Requests.ShouldBeEmpty();
        message.Payload.ShouldBeNull();
    }

    [Test]
    public async Task Hydrate_AlreadyHydratedValueDoesNotFetchPayload()
    {
        // Arrange
        var repository = new Repository();
        await using var provider = Build<JsonMessage>(repository);
        var value = new JsonPayload("existing");
        var message = new JsonMessage { Payload = new MessageData<JsonPayload>(Address, value) };

        // Act
        await Hydrate(provider, message);

        // Assert
        repository.Requests.ShouldBeEmpty();
        message.Payload.Value.ShouldBeSameAs(value);
    }

    [Test]
    public async Task Hydrate_ReadOnlyPropertyDoesNotOpenUnusableStream()
    {
        // Arrange
        var repository = new Repository();
        await using var provider = Build<ReadOnlyMessage>(repository);
        var message = new ReadOnlyMessage();

        // Act
        await Hydrate(provider, message);

        // Assert
        repository.Requests.ShouldBeEmpty();
        message.Payload.HasValue.ShouldBeFalse();
    }

    [Test]
    public async Task Hydrate_MaterializedPayloadAtExactLimitDisposesSource()
    {
        // Arrange
        var repository = new Repository();
        var stream = repository.Store(Address, "1234");
        await using var provider = Build<TextMessage>(repository, maxBytes: 4);
        var message = new TextMessage { Payload = new MessageData<string>(Address) };

        // Act
        await Hydrate(provider, message);

        // Assert
        message.Payload.Value.ShouldBe("1234");
        stream.DisposalCount.ShouldBe(1);
    }

    [Test]
    public async Task Hydrate_SizeLimitFailureDisposesSource()
    {
        // Arrange
        var repository = new Repository();
        var stream = repository.Store(Address, "12345");
        await using var provider = Build<TextMessage>(repository, maxBytes: 4);
        var message = new TextMessage { Payload = new MessageData<string>(Address) };

        // Act
        var exception = await Should.ThrowAsync<InvalidDataException>(() => Hydrate(provider, message));

        // Assert
        exception.Message.ShouldContain("4 byte limit");
        stream.DisposalCount.ShouldBe(1);
        message.Payload.HasValue.ShouldBeFalse();
    }

    [Test]
    public async Task Hydrate_InvalidJsonDisposesSource()
    {
        // Arrange
        var repository = new Repository();
        var stream = repository.Store(Address, "invalid JSON");
        await using var provider = Build<JsonMessage>(repository);
        var message = new JsonMessage { Payload = new MessageData<JsonPayload>(Address) };

        // Act
        var exception = await Should.ThrowAsync<System.Text.Json.JsonException>(() => Hydrate(provider, message));

        // Assert
        exception.ShouldNotBeNull();
        stream.DisposalCount.ShouldBe(1);
    }

    [Test]
    public async Task Hydrate_LaterFailureDisposesPreviouslyOpenedStream()
    {
        // Arrange
        var repository = new Repository();
        var first = repository.Store(Address, "first");
        var second = repository.Store(SecondAddress, "12345");
        await using var provider = Build<MixedMessage>(repository, maxBytes: 4);
        var message = new MixedMessage
        {
            First = new MessageData<Stream>(Address),
            Second = new MessageData<string>(SecondAddress)
        };

        // Act
        var exception = await Should.ThrowAsync<InvalidDataException>(() => Hydrate(provider, message));

        // Assert
        exception.Message.ShouldContain("4 byte limit");
        first.DisposalCount.ShouldBe(1);
        second.DisposalCount.ShouldBe(1);
    }

    [Test]
    public async Task Hydrate_StreamRemainsOpenUntilConsumerDisposesIt()
    {
        // Arrange
        var repository = new Repository();
        var source = repository.Store(Address, "1234");
        await using var provider = Build<StreamMessage>(repository, maxBytes: 4);
        var message = new StreamMessage { Payload = new MessageData<Stream>(Address) };

        // Act
        await Hydrate(provider, message);
        var stream = message.Payload.Value!;
        var bytes = new byte[4];
        var read = await stream.ReadAsync(bytes);
        var end = stream.ReadByte();

        // Assert
        read.ShouldBe(4);
        bytes.ShouldBe(Encoding.UTF8.GetBytes("1234"));
        end.ShouldBe(-1);
        source.DisposalCount.ShouldBe(0);
        await stream.DisposeAsync();
        source.DisposalCount.ShouldBe(1);
    }

    [Test]
    public async Task Hydrate_ForwardsCancellationToRepository()
    {
        // Arrange
        var repository = new Repository();
        await using var provider = Build<TextMessage>(repository);
        var message = new TextMessage { Payload = new MessageData<string>(Address) };
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        // Act
        var exception = await Should.ThrowAsync<OperationCanceledException>(() => Hydrate(provider, message, cancellation.Token));

        // Assert
        repository.Token.ShouldBe(cancellation.Token);
        exception.CancellationToken.ShouldBe(cancellation.Token);
    }

    [Test]
    public async Task Hydrate_ResolvesRepositoryFromDeliveryScope()
    {
        // Arrange
        Repository? resolved = null;
        await using var provider = Build<TextMessage>(repositoryFactory: _ =>
        {
            resolved = new Repository();
            resolved.Store(Address, "scoped");
            return resolved;
        });
        var message = new TextMessage { Payload = new MessageData<string>(Address) };

        // Act
        await Hydrate(provider, message);

        // Assert
        message.Payload.Value.ShouldBe("scoped");
        resolved.ShouldNotBeNull();
        resolved.Disposed.ShouldBeTrue();
    }

    private static ServiceProvider Build<TMessage>(Repository? repository = null, long maxBytes = 64 * 1024 * 1024,
        Func<IServiceProvider, IMessageDataRepository>? repositoryFactory = null) where TMessage : class
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        if (repositoryFactory is not null)
        {
            services.AddScoped(repositoryFactory);
        }
        else if (repository is not null)
        {
            services.AddSingleton<IMessageDataRepository>(repository);
        }
        services.AddConveyo(builder =>
        {
            builder.Map<TMessage>("test:payload.v1");
            builder.AddConsumer<Consumer<TMessage>>();
            builder.MaxMessageDataBytes(maxBytes);
        });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static async Task Hydrate<TMessage>(ServiceProvider provider, TMessage message, CancellationToken cancellationToken = default)
        where TMessage : class
    {
        await using var scope = provider.CreateAsyncScope();
        var dispatch = provider.GetRequiredService<ConveyoContext>().DispatchInfo[typeof(TMessage)];
        await provider.GetRequiredService<MessageDataHydrator>().HydrateAsync(dispatch, message, scope.ServiceProvider, cancellationToken);
    }

    private sealed record JsonPayload(string Value);
    private sealed record JsonMessage
    {
        public MessageData<JsonPayload> Payload { get; init; } = null!;
    }
    private sealed record TextMessage
    {
        public MessageData<string> Payload { get; init; } = null!;
    }
    private sealed record StreamMessage
    {
        public MessageData<Stream> Payload { get; init; } = null!;
    }
    private sealed record ReadOnlyMessage
    {
        public MessageData<Stream> Payload { get; } = new(Address);
    }
    private sealed record MixedMessage
    {
        public MessageData<Stream> First { get; init; } = null!;
        public MessageData<string> Second { get; init; } = null!;
    }
    private sealed class Consumer<TMessage> : IConsumer<TMessage> where TMessage : class
    {
        public Task Consume(ConsumeContext<TMessage> context) => Task.CompletedTask;
    }
    private sealed class Repository : IMessageDataRepository, IAsyncDisposable
    {
        private readonly Dictionary<Uri, TrackingStream> _streams = [];
        public List<Uri> Requests { get; } = [];
        public CancellationToken Token { get; private set; }
        public bool Disposed { get; private set; }
        public TrackingStream Store(Uri address, string text)
        {
            var stream = new TrackingStream(Encoding.UTF8.GetBytes(text));
            _streams.Add(address, stream);
            return stream;
        }
        public Task<Uri> PutAsync(Stream data, TimeSpan? timeToLive = null, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
        public Task<Stream> GetAsync(Uri address, CancellationToken cancellationToken = default)
        {
            Requests.Add(address);
            Token = cancellationToken;
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult<Stream>(_streams[address]);
        }
        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }
    private sealed class TrackingStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        public int DisposalCount { get; private set; }
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                DisposalCount++;
            }
            base.Dispose(disposing);
        }
    }
}
