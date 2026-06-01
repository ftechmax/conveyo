using Microsoft.Extensions.DependencyInjection;

namespace Conveyo;

public interface IConveyoBuilder
{
    IServiceCollection Services { get; }

    internal ConveyoContext Context { get; }

    void AddConsumer<T>() where T : class;

    void MapEndpointConvention<T>(Uri uri) where T : class;

    void Map<T>(string urn) where T : class;

    void MaxMessageDataBytes(long maxBytes);

    void IncludeFaultExceptionDetails(bool include = true);
}

internal sealed class ConveyoBuilder(IServiceCollection services, ConveyoContext context) : IConveyoBuilder
{
    ConveyoContext IConveyoBuilder.Context => context;

    public IServiceCollection Services => services;

    public void AddConsumer<T>() where T : class
    {
        var consumerType = typeof(T);
        var messageTypes = GetConsumerMessageTypes(consumerType);

        context._consumers.Add(consumerType);
        context._consumerMessages[consumerType] = messageTypes;

        foreach (var messageType in messageTypes)
        {
            EnsureDispatchInfo(messageType);
        }

        services.AddScoped<T>();
    }

    private static List<Type> GetConsumerMessageTypes(Type consumerType) =>
        consumerType
            .GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsumer<>))
            .Select(i => i.GetGenericArguments()[0])
            .ToList();

    private void EnsureDispatchInfo(Type messageType)
    {
        if (!context.DispatchInfo.TryGetValue(messageType, out _))
        {
            context.DispatchInfo[messageType] = MessageDispatchBuilder.Build(messageType);
        }
    }

    public void MapEndpointConvention<T>(Uri uri) where T : class
    {
        context._endpointConventions[typeof(T)] = uri;
    }

    public void Map<T>(string urn) where T : class
    {
        context.RegisterUrn(typeof(T), urn);
        if (!IsFaultType(typeof(T)))
        {
            context.RegisterUrn(typeof(Fault<T>), urn + ConveyoContext.FaultUrnSuffix);
        }
    }

    public void MaxMessageDataBytes(long maxBytes)
    {
        if (maxBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBytes), ErrorMessages.MessageDataByteLimitMustBePositive);
        }

        context.MaxMessageDataBytes = maxBytes;
    }

    public void IncludeFaultExceptionDetails(bool include = true)
    {
        context.IncludeFaultExceptionDetails = include;
    }

    private static bool IsFaultType(Type type) =>
        type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Fault<>);
}
