namespace Conveyo;

internal static class QueueAddress
{
    public const string Scheme = "queue";

    public static Uri Create(string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        return new Uri($"{Scheme}:{Uri.EscapeDataString(queueName)}", UriKind.Absolute);
    }

    public static bool IsQueue(Uri address) =>
        address.IsAbsoluteUri
        && string.Equals(address.Scheme, Scheme, StringComparison.OrdinalIgnoreCase);

    public static string GetQueueName(Uri address)
    {
        ArgumentNullException.ThrowIfNull(address);

        if (!IsQueue(address))
        {
            throw new ArgumentException(ErrorMessages.DestinationAddressMustBeQueue(address), nameof(address));
        }

        var value = address.OriginalString;
        var separator = value.IndexOf(':');
        return Uri.UnescapeDataString(value[(separator + 1)..]);
    }

    public static bool NamesEqual(Uri left, Uri right) =>
        string.Equals(GetQueueName(left), GetQueueName(right), StringComparison.Ordinal);
}
