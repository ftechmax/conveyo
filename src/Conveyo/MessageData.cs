#nullable enable
namespace Conveyo;

public class MessageData<T> where T : class
{
    public Uri Address { get; }

    public bool HasValue { get; }

    public T? Value { get; }

    public MessageData(Uri address)
    {
        ArgumentNullException.ThrowIfNull(address);

        Address = address;
        HasValue = false;
        Value = null;
    }

    internal MessageData(Uri address, T value)
    {
        ArgumentNullException.ThrowIfNull(address);
        ArgumentNullException.ThrowIfNull(value);

        Address = address;
        Value = value;
        HasValue = true;
    }
}
