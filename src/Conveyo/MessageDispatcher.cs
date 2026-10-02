using System.Text.Json;
using Conveyo.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Conveyo;

internal sealed class MessageDispatcher(
    ConveyoContext context,
    IServiceProvider serviceProvider,
    MessageDataHydrator hydrator,
    ILogger<MessageDispatcher> logger)
{
    public async Task PublishFaultAsync(MessageEnvelope envelope, IReadOnlyList<Exception> exceptions, CancellationToken cancellationToken)
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

    public async Task DispatchAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
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

            var consumers = GetHandlerTypes(messageType, envelope.DestinationAddress)
                .Select(type => scope.ServiceProvider.GetRequiredService(type)).ToArray();
            var consumeContext = dispatchInfo.CreateConsumeContext(envelope, message, endpointProvider, cancellationToken);
            await hydrator.HydrateAsync(dispatchInfo, message, scope.ServiceProvider, cancellationToken);

            foreach (var consumer in consumers)
            {
                await dispatchInfo.Invoke(consumer, consumeContext);
            }
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

}
