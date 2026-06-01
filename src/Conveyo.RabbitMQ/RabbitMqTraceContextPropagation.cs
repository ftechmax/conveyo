using System.Diagnostics;
using System.Text;
using Conveyo.Diagnostics;

namespace Conveyo.RabbitMQ;

internal static class RabbitMqTraceContextPropagation
{
    public static void Inject(Activity? activity, IDictionary<string, object?> headers)
    {
        if (activity is null)
        {
            return;
        }

        ConveyoActivitySource.Propagator.Inject(activity, headers, static (carrier, key, value) =>
        {
            if (carrier is IDictionary<string, object?> dict)
            {
                dict[key] = Encoding.UTF8.GetBytes(value);
            }
        });
    }

    public static ActivityContext Extract(IDictionary<string, object?>? headers)
    {
        if (headers is null)
        {
            return default;
        }

        ConveyoActivitySource.Propagator.ExtractTraceIdAndState(
            headers,
            static (object? carrier, string fieldName, out string? fieldValue, out IEnumerable<string>? fieldValues) =>
            {
                fieldValue = null;
                fieldValues = null;
                if (carrier is IDictionary<string, object?> dict && dict.TryGetValue(fieldName, out var raw))
                {
                    fieldValue = raw switch
                    {
                        byte[] bytes => Encoding.UTF8.GetString(bytes),
                        string s => s,
                        _ => raw?.ToString()
                    };
                }
            },
            out var traceParent,
            out var traceState);

        if (string.IsNullOrEmpty(traceParent))
        {
            return default;
        }

        return ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var context)
            ? context
            : default;
    }
}
