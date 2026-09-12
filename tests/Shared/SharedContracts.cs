using System.Text.Json;
using System.Text.Json.Nodes;
using NUnit.Framework;

internal static class SharedContracts
{
    // CI supplies the repository directory explicitly. Copied fixtures also support IDE test runs.
    public static string PathFor(string relative) => Path.Combine(
        Environment.GetEnvironmentVariable("CONVEYO_CONTRACTS_DIR")
            ?? Path.Combine(TestContext.CurrentContext.TestDirectory, "contracts"), relative);

    public static JsonElement Read(string relative)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(PathFor(relative)));
        return document.RootElement.Clone();
    }

    public static IEnumerable<TestCaseData> Envelopes()
    {
        foreach (var entry in Read("fixtures/manifest.json").EnumerateArray()
                     .Where(entry => entry.GetProperty("kind").GetString() == "envelope"))
        {
            var file = entry.GetProperty("file").GetString()!;
            yield return new TestCaseData(File.ReadAllBytes(PathFor("fixtures/" + file)), true)
                .SetName("Shared envelope: " + file);
        }
        var suite = Read("fixtures/envelope-cases.json");
        foreach (var test in suite.GetProperty("cases").EnumerateArray())
        {
            JsonNode? envelope;
            if (test.TryGetProperty("root", out var replacement))
            {
                envelope = JsonNode.Parse(replacement.GetRawText());
            }
            else
            {
                envelope = JsonNode.Parse(suite.GetProperty("template").GetRawText())!;
                if (test.TryGetProperty("set", out var fields))
                {
                    foreach (var field in fields.EnumerateObject())
                    {
                        envelope[field.Name] = JsonNode.Parse(field.Value.GetRawText());
                    }
                }

                if (test.TryGetProperty("remove", out var removed))
                {
                    foreach (var field in removed.EnumerateArray())
                    {
                        envelope.AsObject().Remove(field.GetString()!);
                    }
                }

                if (test.TryGetProperty("messageFile", out var file))
                {
                    envelope["message"] = JsonNode.Parse(File.ReadAllText(PathFor("fixtures/" + file.GetString())));
                }
            }
            yield return new TestCaseData(JsonSerializer.SerializeToUtf8Bytes(envelope), test.GetProperty("valid").GetBoolean())
                .SetName("Shared envelope: " + test.GetProperty("name").GetString());
        }
    }

    public static IEnumerable<TestCaseData> Cases(string file) =>
        Read("fixtures/" + file).EnumerateArray()
            .Select(entry => new TestCaseData(entry.Clone()).SetName("Shared contract: " + entry.GetProperty("name").GetString()));
}
