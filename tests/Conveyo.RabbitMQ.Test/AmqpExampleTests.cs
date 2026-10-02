using System.Diagnostics;
using System.Text;
using System.Text.Json.Nodes;
using RabbitMQ.Client;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class AmqpExampleTests
{
    [Test]
    public void ForEnvelope_CommandExample_MatchesSharedAmqpProperties()
    {
        // Arrange
        var example = ReadExample();
        var envelope = EnvelopeSerializer.Deserialize(File.ReadAllBytes(Path.Combine(
            TestContext.CurrentContext.TestDirectory, "contracts", "fixtures", example["envelope"]!.GetValue<string>())));
        var expected = example["properties"]!;

        // Act
        var actual = RabbitMqMessageProperties.ForEnvelope(envelope);

        // Assert
        actual.ContentType.ShouldBe(expected["contentType"]!.GetValue<string>());
        ((int)actual.DeliveryMode).ShouldBe(expected["deliveryMode"]!.GetValue<int>());
        actual.MessageId.ShouldBe(expected["messageId"]!.GetValue<string>());
        actual.CorrelationId.ShouldBe(expected["correlationId"]!.GetValue<string>());
        actual.Type.ShouldBe(expected["type"]!.GetValue<string>());
        actual.Timestamp.UnixTime.ShouldBe(expected["timestamp"]!.GetValue<long>());
        actual.Headers.ShouldNotBeNull();
        actual.Headers.Count.ShouldBe(expected["headers"]!.AsObject().Count);
        actual.Headers["conveyo-version"].ShouldBe(expected["headers"]!["conveyo-version"]!.GetValue<string>());
        actual.Headers.ShouldNotContainKey("tenant-id");
    }

    [Test]
    public void Extract_SharedTraceHeaders_RestoresRemoteParentAndState()
    {
        // Arrange
        var trace = ReadExample()["trace"]!;
        var traceparent = trace["traceparent"]!.GetValue<string>();
        var tracestate = trace["tracestate"]!.GetValue<string>();
        var headers = new Dictionary<string, object?>
        {
            ["traceparent"] = Encoding.UTF8.GetBytes(traceparent),
            ["tracestate"] = tracestate
        };

        // Act
        var context = RabbitMqTraceContextPropagation.Extract(headers);

        // Assert
        context.TraceId.ToString().ShouldBe(traceparent.Split('-')[1]);
        context.SpanId.ToString().ShouldBe(traceparent.Split('-')[2]);
        context.TraceState.ShouldBe(tracestate);
        context.IsRemote.ShouldBeTrue();
    }

    [Test]
    public void Inject_ActivityWithSharedTraceContext_PreservesTraceIdAndState()
    {
        // Arrange
        var trace = ReadExample()["trace"]!;
        var traceparent = trace["traceparent"]!.GetValue<string>();
        var tracestate = trace["tracestate"]!.GetValue<string>();
        using var activity = new Activity("conformance").SetParentId(traceparent).Start();
        activity.TraceStateString = tracestate;
        var headers = new Dictionary<string, object?>();

        // Act
        RabbitMqTraceContextPropagation.Inject(activity, headers);
        var restored = RabbitMqTraceContextPropagation.Extract(headers);

        // Assert
        restored.TraceId.ShouldBe(activity.TraceId);
        restored.SpanId.ShouldBe(activity.SpanId);
        restored.TraceState.ShouldBe(tracestate);
    }

    [Test]
    public async Task Publish_UsesSharedFanoutRouting()
    {
        // Arrange
        var example = ReadExample();
        var urn = example["properties"]!["type"]!.GetValue<string>();
        var channel = TestChannel.Publisher();
        var endpoint = new RabbitMqPublishEndpoint(_ => Task.FromResult(channel.Channel), urn, new HostInfo(), urn);

        // Act
        await endpoint.Publish(new { Value = "test" });

        // Assert
        channel.Publications.Single().Exchange.ShouldBe(example["publish"]!["exchange"]!.GetValue<string>());
        channel.Publications.Single().RoutingKey.ShouldBe(example["publish"]!["routingKey"]!.GetValue<string>());
        channel.Publications.Single().Mandatory.ShouldBe(example["publish"]!["mandatory"]!.GetValue<bool>());
    }

    [Test]
    public async Task Send_UsesSharedMandatoryQueueRouting()
    {
        // Arrange
        var example = ReadExample();
        var urn = example["properties"]!["type"]!.GetValue<string>();
        var queue = example["queue"]!.GetValue<string>();
        var channel = TestChannel.Publisher();
        var endpoint = new RabbitMqSendEndpoint(_ => Task.FromResult(channel.Channel), queue, new HostInfo(), urn);

        // Act
        await endpoint.Send(new { Value = "test" });

        // Assert
        channel.Publications.Single().Exchange.ShouldBe(example["send"]!["exchange"]!.GetValue<string>());
        channel.Publications.Single().RoutingKey.ShouldBe(example["send"]!["routingKey"]!.GetValue<string>());
        channel.Publications.Single().Mandatory.ShouldBe(example["send"]!["mandatory"]!.GetValue<bool>());
    }

    private static JsonNode ReadExample() => JsonNode.Parse(File.ReadAllText(Path.Combine(
        TestContext.CurrentContext.TestDirectory, "contracts", "fixtures", "transport", "rabbitmq.json")))!;

}
