using System.Text;
using System.Text.Json;
using Conveyo.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Conveyo;

internal sealed class ConveyoHostedService(
    ConveyoContext context,
    IBusRegistrationContext busRegistrationContext,
    IServiceProvider serviceProvider,
    ILogger<ConveyoHostedService> logger,
    IMessageDataRepository? messageDataRepository = null) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(LogMessages.Starting);
        busRegistrationContext.OnMessageAsync += OnMessageAsync;
        busRegistrationContext.OnFaultAsync += OnFaultAsync;
        await busRegistrationContext.StartAsync(context, cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(LogMessages.Stopping);
        await busRegistrationContext.StopAsync(cancellationToken);
        busRegistrationContext.OnMessageAsync -= OnMessageAsync;
        busRegistrationContext.OnFaultAsync -= OnFaultAsync;
    }

    private async Task OnFaultAsync(MessageEnvelope envelope, IReadOnlyList<Exception> exceptions, CancellationToken cancellationToken)
    {
        var dispatchInfo = GetFaultDispatchInfo(envelope);
        if (dispatchInfo is null)
        {
            return;
        }

        await using var scope = serviceProvider.CreateAsyncScope();
        var endpointProvider = scope.ServiceProvider.GetRequiredService<IEndpointProvider>();

        using var _ = OutboundContext.Push(new OutboundMetadata(envelope.CorrelationId, envelope.Headers));

        await dispatchInfo.PublishFault(
            envelope,
            exceptions,
            context.HostInfo,
            endpointProvider,
            context.IncludeFaultExceptionDetails,
            cancellationToken);
    }

    private MessageDispatchInfo? GetFaultDispatchInfo(MessageEnvelope envelope)
    {
        if (envelope.MessageType is not { Length: > 0 } messageTypes)
        {
            logger.LogWarning(LogMessages.CannotPublishFaultNoMessageType);
            return null;
        }

        var messageType = ResolveMessageType(messageTypes);

        if (messageType is null || !context.DispatchInfo.TryGetValue(messageType, out var dispatchInfo))
        {
            logger.LogWarning(
                LogMessages.CannotPublishFaultNoDispatchInfo,
                envelope.MessageId, string.Join(", ", messageTypes));
            return null;
        }

        return dispatchInfo;
    }

    private async Task OnMessageAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        try
        {
            ArgumentNullException.ThrowIfNull(envelope);

            var messageType = GetMessageType(envelope);
            var dispatchInfo = GetDispatchInfo(messageType);
            var message = DeserializeMessage(envelope, messageType);

            await using var scope = serviceProvider.CreateAsyncScope();
            var endpointProvider = scope.ServiceProvider.GetRequiredService<IEndpointProvider>();

            // Propagate the inbound correlation id and headers to any Publish/Send performed by the consumer.
            using var _ = OutboundContext.Push(new OutboundMetadata(envelope.CorrelationId, envelope.Headers));

            await HydrateMessageDataAsync(dispatchInfo, message, cancellationToken);

            var consumeContext = dispatchInfo.CreateConsumeContext(envelope, message, endpointProvider, cancellationToken);
            await InvokeConsumersAsync(scope.ServiceProvider, dispatchInfo, messageType, envelope, consumeContext);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, LogMessages.MessageHandlingFailed);
            throw;
        }
    }

    private Type GetMessageType(MessageEnvelope envelope)
    {
        var messageTypes = envelope.MessageType!;

        return ResolveMessageType(messageTypes) ?? throw new MessageNotConsumedException(ErrorMessages.NoTypeRegisteredForUrns(messageTypes));
    }

    private Type? ResolveMessageType(IEnumerable<string?> messageTypes)
        => messageTypes
            .OfType<string>()
            .Select(context.TypeForUrn)
            .FirstOrDefault(t => t is not null);

    private MessageDispatchInfo GetDispatchInfo(Type messageType)
    {
        if (!context.DispatchInfo.TryGetValue(messageType, out var dispatchInfo))
        {
            throw new MessageNotConsumedException(ErrorMessages.NoConsumerRegisteredForMessageType(messageType));
        }

        return dispatchInfo;
    }

    private static object DeserializeMessage(MessageEnvelope envelope, Type messageType)
        => envelope.Message.Deserialize(messageType, ConveyoJsonOptions.Default)
           ?? throw new InvalidOperationException(ErrorMessages.MessageDeserializationFailed);

    private async Task HydrateMessageDataAsync(
        MessageDispatchInfo dispatchInfo,
        object message,
        CancellationToken cancellationToken)
    {
        foreach (var accessor in dispatchInfo.MessageDataProperties)
        {
            if (accessor.Read(message) is not { } access)
            {
                continue;
            }

            logger.LogDebug(LogMessages.ProcessingMessageDataProperty, accessor.PropertyName);

            if (access.HasValue)
            {
                continue;
            }

            var address = access.Address;

            logger.LogDebug(LogMessages.FetchingMessageData, address);
            var value = await ResolveMessageDataAsync(address, accessor.ItemType, cancellationToken);

            if (value == null)
            {
                logger.LogWarning(LogMessages.SkippingMessageDataNullPayload, accessor.PropertyName);
                continue;
            }

            if (accessor.AssignHydrated == null)
            {
                logger.LogWarning(LogMessages.SkippingMessageDataReadOnly, accessor.PropertyName);
                continue;
            }

            logger.LogDebug(LogMessages.SettingMessageDataProperty, accessor.PropertyName);
            accessor.AssignHydrated(message, address, value);
        }
    }

    private async Task InvokeConsumersAsync(
        IServiceProvider scopedServices,
        MessageDispatchInfo dispatchInfo,
        Type messageType,
        MessageEnvelope envelope,
        object consumeContext)
    {
        var handlerTypes = GetHandlerTypes(messageType, envelope.DestinationAddress);

        foreach (var handlerType in handlerTypes)
        {
            logger.LogDebug(LogMessages.ResolvingConsumer, handlerType.FullName);
            var consumerService = scopedServices.GetService(handlerType)
                ?? throw new InvalidOperationException(ErrorMessages.ServiceTypeNotFound(handlerType));

            await dispatchInfo.Invoke(consumerService, consumeContext);
        }
    }

    private IReadOnlyList<Type> GetHandlerTypes(Type messageType, Uri? destinationAddress)
    {
        var handlerTypes = context.GetHandlersByMessage(messageType, destinationAddress);
        if (handlerTypes.Count > 0)
        {
            return handlerTypes;
        }

        throw new MessageNotConsumedException(
            destinationAddress is null
                ? ErrorMessages.NoHandlerFoundForMessageType(messageType)
                : ErrorMessages.NoHandlerFoundForMessageTypeAtDestination(messageType, destinationAddress));
    }

    private async Task<object?> ResolveMessageDataAsync(Uri address, Type messageDataItemType, CancellationToken cancellationToken)
    {
        // Conveyo supports base64-encoded data URIs for inline MessageData payloads.
        if (string.Equals(address.Scheme, "data", StringComparison.OrdinalIgnoreCase))
        {
            var payload = DataUri.Decode(address, context.MaxMessageDataBytes);
            return await DecodePayloadAsync(payload, messageDataItemType, cancellationToken);
        }

        var repository = messageDataRepository
            ?? throw new InvalidOperationException(ErrorMessages.CannotHydrateMessageDataWithoutRepository(address));
        var dataStream = new BoundedReadStream(
            await repository.GetAsync(address, cancellationToken),
            context.MaxMessageDataBytes,
            address);

        return await DecodePayloadAsync(dataStream, messageDataItemType, cancellationToken);
    }

    private static async Task<object?> DecodePayloadAsync(Stream payload, Type messageDataItemType, CancellationToken cancellationToken)
    {
        if (messageDataItemType == typeof(Stream))
        {
            return payload;
        }

        await using var _ = payload;

        if (messageDataItemType == typeof(string))
        {
            using var reader = new StreamReader(payload, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
            return await reader.ReadToEndAsync(cancellationToken);
        }

        if (messageDataItemType == typeof(byte[]))
        {
            using var memoryStream = new MemoryStream();
            await payload.CopyToAsync(memoryStream, cancellationToken);
            return memoryStream.ToArray();
        }

        return await JsonSerializer.DeserializeAsync(payload, messageDataItemType, ConveyoJsonOptions.Default, cancellationToken);
    }

    // Same idea as Kestrel request stream: https://github.com/dotnet/aspnetcore/blob/main/src/Servers/Kestrel/Core/src/Internal/Http/HttpRequestStream.cs
    private sealed class BoundedReadStream(Stream inner, long maxBytes, Uri address) : Stream
    {
        private readonly Stream _inner = inner;
        private readonly long _maxBytes = maxBytes;
        private readonly Uri _address = address;
        private long _remaining = maxBytes;

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            ValidateReadArguments(buffer, offset, count);
            if (count == 0)
            {
                return 0;
            }

            if (_remaining == 0)
            {
                var extra = _inner.ReadByte();
                if (extra < 0)
                {
                    return 0;
                }

                throw CreateLimitException();
            }

            var bytesToRead = (int)Math.Min(count, _remaining);
            var read = _inner.Read(buffer, offset, bytesToRead);
            _remaining -= read;
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (buffer.Length == 0)
            {
                return 0;
            }

            if (_remaining == 0)
            {
                var extra = await _inner.ReadAsync(buffer[..1], cancellationToken);
                if (extra == 0)
                {
                    return 0;
                }

                throw CreateLimitException();
            }

            var bytesToRead = (int)Math.Min(buffer.Length, _remaining);
            var read = await _inner.ReadAsync(buffer[..bytesToRead], cancellationToken);
            _remaining -= read;
            return read;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync();
            await base.DisposeAsync();
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        private InvalidDataException CreateLimitException() =>
            new(ErrorMessages.MessageDataPayloadExceedsByteLimit(_address, _maxBytes));

        private static void ValidateReadArguments(byte[] buffer, int offset, int count)
        {
            ArgumentNullException.ThrowIfNull(buffer);
            if (offset < 0 || count < 0 || buffer.Length - offset < count)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
        }
    }
}
