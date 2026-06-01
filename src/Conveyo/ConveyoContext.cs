using System.Text;
using System.Text.RegularExpressions;

namespace Conveyo;

internal sealed partial record ConveyoContext
{
    public const string FaultUrnSuffix = ".fault";

    [GeneratedRegex(@"\A[A-Za-z0-9._:\-]+\z")]
    private static partial Regex UrnCharacterSet();

    internal readonly List<Type> _consumers = [];
    internal readonly Dictionary<Type, List<Type>> _consumerMessages = [];
    internal readonly Dictionary<Type, List<Uri>> _consumerEndpoints = [];
    internal readonly Dictionary<Type, Uri> _endpointConventions = [];
    internal readonly Dictionary<string, Type> _messageTypeLookup = [];
    internal readonly Dictionary<Type, string> _urnsByType = [];
    internal readonly Dictionary<Type, MessageDispatchInfo> DispatchInfo = [];

    internal long MaxMessageDataBytes { get; set; } = ConveyoDefaults.MaxMessageDataBytes;

    internal bool IncludeFaultExceptionDetails { get; set; }

    public IReadOnlyList<Type> Consumers => _consumers;
    public IReadOnlyDictionary<Type, List<Type>> ConsumerMessages => _consumerMessages;
    public IReadOnlyDictionary<Type, List<Uri>> ConsumerEndpoints => _consumerEndpoints;
    public IReadOnlyDictionary<Type, Uri> EndpointConventions => _endpointConventions;
    public IReadOnlyDictionary<string, Type> MessageTypeLookup => _messageTypeLookup;
    public IReadOnlyDictionary<Type, string> UrnsByType => _urnsByType;

    public required HostInfo HostInfo { get; init; }

    public string UrnFor(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        if (!_urnsByType.TryGetValue(type, out var urn))
        {
            throw new InvalidOperationException(ErrorMessages.MessageTypeHasNoUrnMapping(type));
        }

        return urn;
    }

    public Type? TypeForUrn(string urn) => _messageTypeLookup.GetValueOrDefault(urn);

    internal void RegisterUrn(Type type, string urn)
    {
        ArgumentNullException.ThrowIfNull(type);
        ValidateUrn(urn);

        if (_messageTypeLookup.TryGetValue(urn, out var existingType) && existingType != type)
        {
            throw new InvalidOperationException(ErrorMessages.UrnAlreadyRegistered(urn, existingType, type));
        }

        if (_urnsByType.TryGetValue(type, out var previousUrn) && !string.Equals(previousUrn, urn, StringComparison.Ordinal))
        {
            _messageTypeLookup.Remove(previousUrn);
        }

        _urnsByType[type] = urn;
        _messageTypeLookup[urn] = type;
    }

    internal static void ValidateUrn(string urn)
    {
        if (string.IsNullOrEmpty(urn))
        {
            throw new ArgumentException(ErrorMessages.UrnRequired, nameof(urn));
        }

        if (Encoding.UTF8.GetByteCount(urn) > 255)
        {
            throw new ArgumentException(ErrorMessages.UrnExceedsAmqpLimit(urn), nameof(urn));
        }

        if (!UrnCharacterSet().IsMatch(urn))
        {
            throw new ArgumentException(ErrorMessages.UrnContainsInvalidCharacters(urn), nameof(urn));
        }
    }

    internal IReadOnlyList<Type> GetHandlersByMessage(Type type, Uri? destinationAddress = null)
    {
        var handlers = _consumerMessages
            .Where(kvp => kvp.Value.Contains(type))
            .Select(kvp => kvp.Key)
            .ToList();

        if (destinationAddress is null)
        {
            return handlers;
        }

        var hasEndpointRegistrations = false;
        var endpointHandlers = new List<Type>();
        foreach (var handler in handlers)
        {
            if (!_consumerEndpoints.TryGetValue(handler, out var endpoints))
            {
                continue;
            }

            hasEndpointRegistrations = true;
            if (endpoints.Any(endpoint => EndpointAddressesEqual(endpoint, destinationAddress)))
            {
                endpointHandlers.Add(handler);
            }
        }

        return hasEndpointRegistrations ? endpointHandlers : handlers;
    }

    private static bool EndpointAddressesEqual(Uri configuredAddress, Uri deliveryAddress)
    {
        if (QueueAddress.IsQueue(configuredAddress) && QueueAddress.IsQueue(deliveryAddress))
        {
            return QueueAddress.NamesEqual(configuredAddress, deliveryAddress);
        }

        return Uri.Compare(
            configuredAddress,
            deliveryAddress,
            UriComponents.AbsoluteUri,
            UriFormat.Unescaped,
            StringComparison.OrdinalIgnoreCase) == 0;
    }
}
