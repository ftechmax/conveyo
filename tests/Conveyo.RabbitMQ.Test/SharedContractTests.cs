using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Conveyo.Serialization;
using RabbitMQ.Client;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
internal class SharedContractTests
{
    public static IEnumerable<TestCaseData> Envelopes() => SharedContracts.Envelopes();

    [TestCaseSource(nameof(Envelopes))]
    public void EnvelopeAcceptance(byte[] bytes, bool valid)
    {
        if (valid)
        {
            Assert.That(EnvelopeSerializer.Deserialize(bytes).Message.ValueKind, Is.EqualTo(JsonValueKind.Object));
        }
        else
        {
            Assert.Throws<EnvelopeDeserializationException>(() => EnvelopeSerializer.Deserialize(bytes));
        }
    }

    [Test]
    public void PayloadPreservesConcreteNumbersEnumNullCollectionAndBinary()
    {
        var json = SharedContracts.Read("fixtures/payloads/types.json");
        var payload = json.Deserialize<PayloadExample>(ConveyoJsonOptions.Default)!;
        Assert.Multiple(() =>
        {
            Assert.That(payload.Sequence, Is.EqualTo(long.MaxValue));
            Assert.That(payload.Amount, Is.EqualTo(1234567890.125m));
            Assert.That(payload.Kind, Is.EqualTo(SampleKind.Humidity));
            Assert.That(payload.Note, Is.Null);
            Assert.That(payload.Readings, Is.EqualTo(new decimal[] { 1, 2.5m, -3 }));
            Assert.That(payload.Binary, Is.EqualTo(new byte[] { 0, 1, 254, 255 }));
        });
        Assert.That(JsonNode.DeepEquals(JsonNode.Parse(json.GetRawText()),
            JsonSerializer.SerializeToNode(payload, ConveyoJsonOptions.Default)), Is.True);
    }

    [Test]
    public void FaultShapeAndRedactionMatchFixture()
    {
        var json = SharedContracts.Read("fixtures/payloads/fault.json");
        var fault = json.Deserialize<Fault<PayloadExample>>(ConveyoJsonOptions.Default)!;
        var redacted = ExceptionInfo.From(new InvalidOperationException("private", new Exception("inner")));
        Assert.That(JsonNode.DeepEquals(JsonNode.Parse(json.GetProperty("exceptions")[0].GetRawText()),
            JsonSerializer.SerializeToNode(redacted, ConveyoJsonOptions.Default)), Is.True);
        Assert.That(fault.Message.Sequence, Is.EqualTo(long.MaxValue));
        Assert.That(fault.Timestamp.Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(fault.FaultedMessageId, Is.EqualTo(Guid.Parse("11111111-2222-3333-4444-555555555555")));
    }

    [Test]
    public void AmqpMetadataAndTraceMatchSharedFixture()
    {
        var fixture = SharedContracts.Read("fixtures/transport/rabbitmq.json");
        var envelope = EnvelopeSerializer.Deserialize(File.ReadAllBytes(
            SharedContracts.PathFor("fixtures/" + fixture.GetProperty("envelope").GetString())));
        var expected = fixture.GetProperty("properties");
        var actual = RabbitMqMessageProperties.ForEnvelope(envelope);
        Assert.Multiple(() =>
        {
            Assert.That(actual.ContentType, Is.EqualTo(expected.GetProperty("contentType").GetString()));
            Assert.That((int)actual.DeliveryMode, Is.EqualTo(expected.GetProperty("deliveryMode").GetInt32()));
            Assert.That(actual.MessageId, Is.EqualTo(expected.GetProperty("messageId").GetString()));
            Assert.That(actual.CorrelationId, Is.EqualTo(expected.GetProperty("correlationId").GetString()));
            Assert.That(actual.Type, Is.EqualTo(expected.GetProperty("type").GetString()));
            Assert.That(actual.Timestamp.UnixTime, Is.EqualTo(expected.GetProperty("timestamp").GetInt64()));
            Assert.That(actual.Headers!["conveyo-version"], Is.EqualTo("1"));
            Assert.That(actual.Headers.ContainsKey("tenant-id"), Is.False);
        });
        var trace = fixture.GetProperty("trace");
        var traceparent = trace.GetProperty("traceparent").GetString()!;
        var tracestate = trace.GetProperty("tracestate").GetString()!;
        var context = RabbitMqTraceContextPropagation.Extract(new Dictionary<string, object?>
        {
            ["traceparent"] = System.Text.Encoding.UTF8.GetBytes(traceparent),
            ["tracestate"] = tracestate
        });
        Assert.That(context.TraceId.ToString(), Is.EqualTo(traceparent.Split('-')[1]));
        Assert.That(context.TraceState, Is.EqualTo(tracestate));
        using var activity = new Activity("conformance").SetParentId(traceparent).Start();
        activity.TraceStateString = tracestate;
        var headers = new Dictionary<string, object?>();
        RabbitMqTraceContextPropagation.Inject(activity, headers);
        var restored = RabbitMqTraceContextPropagation.Extract(headers);
        Assert.That(restored.TraceId, Is.EqualTo(context.TraceId));
        Assert.That(restored.TraceState, Is.EqualTo(tracestate));
    }

    [Test]
    public async Task PublishAndSendUseSharedRoutingExpectations()
    {
        var fixture = SharedContracts.Read("fixtures/transport/rabbitmq.json");
        var urn = fixture.GetProperty("properties").GetProperty("type").GetString()!;
        var queue = fixture.GetProperty("queue").GetString()!;
        var channel = DispatchProxy.Create<IChannel, RoutingChannel>();
        var capture = (RoutingChannel)(object)channel;
        var publish = new RabbitMqPublishEndpoint(_ => Task.FromResult(channel), urn, new HostInfo(), urn);
        await publish.Publish(new { Value = "test" });
        AssertRoute(fixture.GetProperty("publish"), capture);
        var send = new RabbitMqSendEndpoint(_ => Task.FromResult(channel), queue, new HostInfo(), urn);
        await send.Send(new { Value = "test" });
        AssertRoute(fixture.GetProperty("send"), capture);
    }

    private static void AssertRoute(JsonElement expected, RoutingChannel actual)
    {
        Assert.That(actual.Exchange, Is.EqualTo(expected.GetProperty("exchange").GetString()));
        Assert.That(actual.RoutingKey, Is.EqualTo(expected.GetProperty("routingKey").GetString()));
        Assert.That(actual.Mandatory, Is.EqualTo(expected.GetProperty("mandatory").GetBoolean()));
    }

    private class RoutingChannel : DispatchProxy
    {
        public string? Exchange { get; private set; }
        public string? RoutingKey { get; private set; }
        public bool Mandatory { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod!.Name == nameof(IChannel.BasicPublishAsync))
            {
                Exchange = (string)args![0]!;
                RoutingKey = (string)args[1]!;
                Mandatory = (bool)args[2]!;
                return ValueTask.CompletedTask;
            }
            if (targetMethod.Name == nameof(IAsyncDisposable.DisposeAsync))
            {
                return ValueTask.CompletedTask;
            }
            throw new NotSupportedException(targetMethod.Name);
        }
    }

    private enum SampleKind
    {
        Temperature,
        Humidity
    }
    private sealed record PayloadExample(long Sequence, decimal Amount, SampleKind Kind, string? Note, decimal[] Readings, byte[] Binary);

}
