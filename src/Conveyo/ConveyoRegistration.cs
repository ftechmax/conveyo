using System.Collections.ObjectModel;

namespace Conveyo;

internal sealed class ConveyoRegistration
{
    private readonly Dictionary<Type, IReadOnlyList<Type>> _consumerMessages = [];
    private readonly Dictionary<Type, List<Uri>> _consumerEndpoints = [];
    private readonly Dictionary<Type, Uri> _endpointConventions = [];
    private readonly Dictionary<string, Type> _messageTypeLookup = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, string> _urnsByType = [];
    private long _maxMessageDataBytes = ConveyoDefaults.MaxMessageDataBytes;
    private bool _includeFaultExceptionDetails;
    private ConveyoContext? _context;

    public ConveyoContext Context => _context
        ?? throw new InvalidOperationException("Conveyo registration has not completed.");

    public void EnsureOpen()
    {
        if (_context is not null)
        {
            throw new InvalidOperationException("Conveyo configuration must be completed inside AddConveyo.");
        }
    }

    public bool AddConsumer(Type consumerType)
    {
        EnsureOpen();
        var messageTypes = consumerType.GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsumer<>))
            .Select(i => i.GetGenericArguments()[0])
            .ToArray();
        if (messageTypes.Length == 0)
        {
            throw new ArgumentException($"{consumerType.FullName} must implement IConsumer<T>.", nameof(consumerType));
        }
        return _consumerMessages.TryAdd(consumerType, Array.AsReadOnly(messageTypes));
    }

    public void RegisterConsumerEndpoint(Type consumerType, Uri address)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(address);
        if (!address.IsAbsoluteUri)
        {
            throw new ArgumentException("Consumer endpoint addresses must be absolute.", nameof(address));
        }
        if (!_consumerEndpoints.TryGetValue(consumerType, out var endpoints))
        {
            endpoints = [];
            _consumerEndpoints.Add(consumerType, endpoints);
        }
        if (!endpoints.Contains(address))
        {
            endpoints.Add(address);
        }
    }

    public void MapEndpointConvention(Type messageType, Uri uri)
    {
        EnsureOpen();
        ArgumentNullException.ThrowIfNull(uri);
        if (!uri.IsAbsoluteUri)
        {
            throw new ArgumentException("Endpoint convention addresses must be absolute.", nameof(uri));
        }
        _endpointConventions[messageType] = uri;
    }

    public void Map(Type messageType, string urn)
    {
        EnsureOpen();
        ValidateMapping(messageType, urn);
        var isFault = messageType.IsGenericType && messageType.GetGenericTypeDefinition() == typeof(Fault<>);
        var faultType = isFault ? null : typeof(Fault<>).MakeGenericType(messageType);
        if (faultType is not null)
        {
            ValidateMapping(faultType, urn + ConveyoContext.FaultUrnSuffix, messageType);
        }
        RegisterUrn(messageType, urn);
        if (faultType is not null)
        {
            RegisterUrn(faultType, urn + ConveyoContext.FaultUrnSuffix);
        }
    }

    private void ValidateMapping(Type type, string urn, Type? remappedMessageType = null)
    {
        ConveyoContext.ValidateUrn(urn);
        if (_messageTypeLookup.TryGetValue(urn, out var existingType) && existingType != type && existingType != remappedMessageType)
        {
            throw new InvalidOperationException(ErrorMessages.UrnAlreadyRegistered(urn, existingType, type));
        }
    }

    private void RegisterUrn(Type type, string urn)
    {
        if (_urnsByType.TryGetValue(type, out var previousUrn))
        {
            _messageTypeLookup.Remove(previousUrn);
        }
        _urnsByType[type] = urn;
        _messageTypeLookup[urn] = type;
    }

    public void MaxMessageDataBytes(long maxBytes)
    {
        EnsureOpen();
        if (maxBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBytes), ErrorMessages.MessageDataByteLimitMustBePositive);
        }
        _maxMessageDataBytes = maxBytes;
    }

    public void IncludeFaultExceptionDetails(bool include)
    {
        EnsureOpen();
        _includeFaultExceptionDetails = include;
    }

    public ConveyoContext Build(HostInfo hostInfo)
    {
        EnsureOpen();
        var unmapped = _consumerMessages.Values.SelectMany(messages => messages)
            .Distinct().Where(type => !_urnsByType.ContainsKey(type)).ToArray();
        if (unmapped.Length > 0)
        {
            throw new InvalidOperationException(ErrorMessages.ConsumedMessageTypesHaveNoUrnMapping(unmapped));
        }
        foreach (var consumerType in _consumerEndpoints.Keys)
        {
            if (!_consumerMessages.ContainsKey(consumerType))
            {
                throw new InvalidOperationException($"Endpoint consumer {consumerType.FullName} is not registered. Call AddConsumer<{consumerType.Name}>().");
            }
        }
        _context = new ConveyoContext
        {
            HostInfo = hostInfo,
            Consumers = Array.AsReadOnly(_consumerMessages.Keys.ToArray()),
            ConsumerMessages = Snapshot(_consumerMessages),
            ConsumerEndpoints = Snapshot(_consumerEndpoints.ToDictionary(pair => pair.Key,
                pair => (IReadOnlyList<Uri>)Array.AsReadOnly(pair.Value.ToArray()))),
            EndpointConventions = Snapshot(_endpointConventions),
            MessageTypeLookup = Snapshot(_messageTypeLookup),
            UrnsByType = Snapshot(_urnsByType),
            DispatchInfo = Snapshot(_consumerMessages.Values.SelectMany(types => types).Distinct()
                .ToDictionary(type => type, MessageDispatchBuilder.Build)),
            MaxMessageDataBytes = _maxMessageDataBytes,
            IncludeFaultExceptionDetails = _includeFaultExceptionDetails
        };
        return _context;
    }

    private static IReadOnlyDictionary<TKey, TValue> Snapshot<TKey, TValue>(Dictionary<TKey, TValue> source)
        where TKey : notnull => new ReadOnlyDictionary<TKey, TValue>(new Dictionary<TKey, TValue>(source, source.Comparer));
}
