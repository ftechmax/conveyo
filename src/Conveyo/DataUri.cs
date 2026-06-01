namespace Conveyo;

internal static class DataUri
{
    public static Stream Decode(Uri uri, long? maxBytes = null)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (maxBytes is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBytes), ErrorMessages.ByteLimitMustBeNonNegative);
        }

        if (!string.Equals(uri.Scheme, "data", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(ErrorMessages.ExpectedDataUri(uri), nameof(uri));
        }

        // Use OriginalString so the payload reaches the base64 decoder exactly as it appeared in
        // the envelope. Supported format: data:[<mediatype>];base64,<data>
        var raw = uri.OriginalString;
        var schemeSep = raw.IndexOf(':');
        if (schemeSep < 0)
        {
            throw new FormatException(ErrorMessages.MalformedDataUriMissingColon);
        }

        var metadataStart = schemeSep + 1;
        var commaIndex = raw.IndexOf(',', metadataStart);
        if (commaIndex < 0)
        {
            throw new FormatException(ErrorMessages.MalformedDataUriMissingComma);
        }

        var metadata = raw[metadataStart..commaIndex];
        if (!HasBase64Parameter(metadata))
        {
            throw new FormatException(ErrorMessages.MalformedDataUriRequiresBase64);
        }

        var payload = raw[(commaIndex + 1)..];
        var bytes = DecodeBase64(payload, maxBytes);
        return new MemoryStream(bytes, writable: false);
    }

    private static byte[] DecodeBase64(string payload, long? maxBytes)
    {
        if (maxBytes is { } limit)
        {
            var decodedLength = GetBase64DecodedLength(payload);
            if (decodedLength > limit)
            {
                throw CreateLimitException(limit);
            }
        }

        var bytes = Convert.FromBase64String(payload);
        if (maxBytes is { } max && bytes.LongLength > max)
        {
            throw CreateLimitException(max);
        }

        return bytes;
    }

    private static long GetBase64DecodedLength(string payload)
    {
        long nonWhitespaceChars = 0;
        foreach (var c in payload)
        {
            if (!char.IsWhiteSpace(c))
            {
                nonWhitespaceChars++;
            }
        }

        if (nonWhitespaceChars == 0 || nonWhitespaceChars % 4 != 0)
        {
            return 0;
        }

        var padding = 0;
        for (var i = payload.Length - 1; i >= 0; i--)
        {
            var c = payload[i];
            if (char.IsWhiteSpace(c))
            {
                continue;
            }

            if (c != '=')
            {
                break;
            }

            padding++;
        }

        return (nonWhitespaceChars / 4 * 3) - Math.Min(padding, 2);
    }

    private static bool HasBase64Parameter(string metadata)
    {
        var firstSeparator = metadata.IndexOf(';');
        if (firstSeparator < 0)
        {
            return false;
        }

        foreach (var parameter in metadata[(firstSeparator + 1)..].Split(';'))
        {
            if (string.Equals(parameter, "base64", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static InvalidDataException CreateLimitException(long maxBytes) =>
        new(ErrorMessages.DataUriPayloadExceedsByteLimit(maxBytes));
}
