using System.Text;
using System.Text.RegularExpressions;

namespace Conveyo;

internal sealed partial class ConveyoContext
{
    public const string FaultUrnSuffix = ".fault";

    [GeneratedRegex(@"\A[A-Za-z0-9._:\-]+\z")]
    private static partial Regex UrnCharacterSet();

    public required IReadOnlyList<Type> Consumers { get; init; }
    public required IReadOnlyDictionary<Type, IReadOnlyList<Type>> ConsumerMessages { get; init; }
    public required IReadOnlyDictionary<Type, IReadOnlyList<Uri>> ConsumerEndpoints { get; init; }
    public required IReadOnlyDictionary<Type, Uri> EndpointConventions { get; init; }
    public required IReadOnlyDictionary<string, Type> MessageTypeLookup { get; init; }
    public required IReadOnlyDictionary<Type, string> UrnsByType { get; init; }
    public required IReadOnlyDictionary<Type, MessageDispatchInfo> DispatchInfo { get; init; }
    public required long MaxMessageDataBytes { get; init; }
    public required bool IncludeFaultExceptionDetails { get; init; }
    public required HostInfo HostInfo { get; init; }

    public string UrnFor(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        if (!UrnsByType.TryGetValue(type, out var urn))
        {
            throw new InvalidOperationException(ErrorMessages.MessageTypeHasNoUrnMapping(type));
        }

        return urn;
    }

    public Type? TypeForUrn(string urn) => MessageTypeLookup.GetValueOrDefault(urn);

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
        var handlers = ConsumerMessages
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
            if (!ConsumerEndpoints.TryGetValue(handler, out var endpoints))
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
