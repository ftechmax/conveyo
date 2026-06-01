using System.Reflection;
using System.Text.Json;
using Conveyo.Serialization;

namespace Conveyo;

internal sealed record MessageDispatchInfo(
    Func<MessageEnvelope, object, IEndpointProvider, CancellationToken, object> CreateConsumeContext,
    Func<object, object, Task> Invoke,
    IReadOnlyList<MessageDataPropertyAccessor> MessageDataProperties,
    Func<MessageEnvelope, IReadOnlyList<Exception>, HostInfo, IEndpointProvider, bool, CancellationToken, Task> PublishFault);

internal sealed record MessageDataPropertyAccessor(
    string PropertyName,
    Type ItemType,
    Func<object, MessageDataAccess?> Read,
    Action<object, Uri, object>? AssignHydrated);

internal readonly record struct MessageDataAccess(Uri Address, bool HasValue);

internal static class MessageDispatchBuilder
{
    public static MessageDispatchInfo Build(Type messageType) => new(
        CreateConsumeContext: BindGeneric<Func<MessageEnvelope, object, IEndpointProvider, CancellationToken, object>>(
            nameof(CreateConsumeContext), messageType),
        Invoke: BindGeneric<Func<object, object, Task>>(
            nameof(InvokeConsumer), messageType),
        MessageDataProperties: BuildMessageDataAccessors(messageType),
        PublishFault: BindGeneric<Func<MessageEnvelope, IReadOnlyList<Exception>, HostInfo, IEndpointProvider, bool, CancellationToken, Task>>(
            nameof(PublishFault), messageType));

    private static TDelegate BindGeneric<TDelegate>(string methodName, params Type[] typeArguments)
        where TDelegate : Delegate
    {
        return typeof(MessageDispatchBuilder)
            .GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeArguments)
            .CreateDelegate<TDelegate>();
    }

    private static object CreateConsumeContext<T>(MessageEnvelope envelope, object message, IEndpointProvider provider, CancellationToken cancellationToken)
        where T : class
        => new ConsumeContextImpl<T>(envelope, (T)message, provider, cancellationToken);

    private static Task InvokeConsumer<T>(object consumer, object context) where T : class
        => ((IConsumer<T>)consumer).Consume((ConsumeContext<T>)context);

    private static async Task PublishFault<T>(
        MessageEnvelope envelope,
        IReadOnlyList<Exception> exceptions,
        HostInfo host,
        IEndpointProvider provider,
        bool includeExceptionDetails,
        CancellationToken cancellationToken) where T : class
    {
        var message = envelope.Message.Deserialize<T>(ConveyoJsonOptions.Default);
        if (message is null)
        {
            return;
        }

        var fault = new Fault<T>
        {
            FaultId = Guid.NewGuid(),
            FaultedMessageId = envelope.MessageId,
            Timestamp = DateTime.UtcNow,
            Exceptions = exceptions.Select(ex => ExceptionInfo.From(ex, includeExceptionDetails)).ToArray(),
            Host = host,
            Message = message
        };

        var publishEndpoint = provider.GetPublishEndpoint<Fault<T>>();
        await publishEndpoint.Publish(fault, cancellationToken);
    }

    private static IReadOnlyList<MessageDataPropertyAccessor> BuildMessageDataAccessors(Type messageType)
    {
        var accessors = new List<MessageDataPropertyAccessor>();
        foreach (var property in messageType.GetProperties())
        {
            if (!property.PropertyType.IsGenericType ||
                property.PropertyType.GetGenericTypeDefinition() != typeof(MessageData<>))
            {
                continue;
            }

            var itemType = property.PropertyType.GetGenericArguments()[0];
            var build = BindGeneric<Func<PropertyInfo, MessageDataPropertyAccessor>>(
                nameof(BuildMessageDataAccessor), messageType, itemType);
            accessors.Add(build(property));
        }
        return accessors;
    }

    private static MessageDataPropertyAccessor BuildMessageDataAccessor<TMessage, TItem>(PropertyInfo property)
        where TMessage : class
        where TItem : class
    {
        var getter = property.GetMethod!.CreateDelegate<Func<TMessage, MessageData<TItem>?>>();
        var setter = property.SetMethod?.CreateDelegate<Action<TMessage, MessageData<TItem>>>();

        return new MessageDataPropertyAccessor(
            PropertyName: property.Name,
            ItemType: typeof(TItem),
            Read: message =>
            {
                var messageData = getter((TMessage)message);
                return messageData is null
                    ? null
                    : new MessageDataAccess(messageData.Address, messageData.HasValue);
            },
            AssignHydrated: setter is null ? null : (message, uri, value) =>
                setter((TMessage)message, new MessageData<TItem>(uri, (TItem)value)));
    }
}
