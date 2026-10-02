using System.Text;
using System.Text.Json;
using Conveyo.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Conveyo;

internal sealed class MessageDataHydrator(ConveyoContext context, ILogger<MessageDataHydrator> logger)
{
    public async Task HydrateAsync(
        MessageDispatchInfo dispatchInfo,
        object message,
        IServiceProvider scopedServices,
        CancellationToken cancellationToken)
    {
        var openedStreams = new List<Stream>();
        try
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

                if (accessor.AssignHydrated == null)
                {
                    logger.LogWarning(LogMessages.SkippingMessageDataReadOnly, accessor.PropertyName);
                    continue;
                }

                var address = access.Address;

                logger.LogDebug(LogMessages.FetchingMessageData, address);
                var value = await ResolveMessageDataAsync(address, accessor.ItemType, scopedServices, cancellationToken);

                if (value == null)
                {
                    logger.LogWarning(LogMessages.SkippingMessageDataNullPayload, accessor.PropertyName);
                    continue;
                }

                if (value is Stream stream)
                {
                    openedStreams.Add(stream);
                }

                logger.LogDebug(LogMessages.SettingMessageDataProperty, accessor.PropertyName);
                accessor.AssignHydrated(message, address, value);
            }
        }
        catch
        {
            foreach (var stream in openedStreams)
            {
                await stream.DisposeAsync();
            }
            throw;
        }
    }

    private async Task<object?> ResolveMessageDataAsync(Uri address, Type messageDataItemType, IServiceProvider scopedServices, CancellationToken cancellationToken)
    {
        // Conveyo supports base64-encoded data URIs for inline MessageData payloads.
        if (string.Equals(address.Scheme, "data", StringComparison.OrdinalIgnoreCase))
        {
            var payload = DataUri.Decode(address, context.MaxMessageDataBytes);
            return await DecodePayloadAsync(payload, messageDataItemType, cancellationToken);
        }

        var repository = scopedServices.GetService<IMessageDataRepository>()
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

    private sealed class BoundedReadStream(Stream inner, long maxBytes, Uri address) : Stream
    {
        private readonly Stream _inner = inner;
        private readonly long _maxBytes = maxBytes;
        private readonly Uri _address = address;
        private long _remaining = maxBytes;
        private bool _disposed;

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
            if (disposing && !_disposed)
            {
                _disposed = true;
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (!_disposed)
            {
                _disposed = true;
                await _inner.DisposeAsync();
            }
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
