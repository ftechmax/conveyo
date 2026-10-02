using System.Text.Json.Nodes;

namespace Conveyo.RabbitMQ.Test;

internal static class JsonAssertions
{
    /// <summary>
    /// Compares parsed JSON values, so whitespace, property order, and equivalent number
    /// formatting do not affect the result.
    /// </summary>
    public static void ShouldBeJson(this JsonNode? actual, string expectedJson)
    {
        JsonNode.DeepEquals(actual, JsonNode.Parse(expectedJson))
            .ShouldBeTrue($"Expected JSON:\n{expectedJson}\nActual JSON:\n{actual?.ToJsonString() ?? "null"}");
    }
}
