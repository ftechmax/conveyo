using System.Diagnostics;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class TracingTests
{
    private List<Activity> _stoppedActivities = null!;
    private ActivityListener _listener = null!;

    [SetUp]
    public void SetUp()
    {
        _stoppedActivities = [];
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Conveyo",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => _stoppedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(_listener);
    }

    [TearDown]
    public void TearDown()
    {
        _listener.Dispose();
    }

    [Test]
    public async Task Send_EmitsProducerActivityAndInjectsTraceparent()
    {
        // Arrange
        var publisher = TestChannel.Publisher();
        var endpoint = new RabbitMqSendEndpoint(
            _ => Task.FromResult(publisher.Channel),
            queueName: "orders",
            hostInfo: new HostInfo(),
            urn: "conveyo:test.example.v1");

        // Act
        await endpoint.Send(new ExampleMessage("hello"));

        // Assert
        var producer = _stoppedActivities.Single();
        producer.Kind.ShouldBe(ActivityKind.Producer);
        producer.OperationName.ShouldBe("orders send");
        producer.GetTagItem("messaging.system").ShouldBe(RabbitMqDiagnosticHeaders.MessagingSystem);
        producer.GetTagItem("messaging.operation.type").ShouldBe("send");
        producer.GetTagItem("messaging.destination.name").ShouldBe("orders");
        producer.GetTagItem(RabbitMqDiagnosticHeaders.RoutingKey).ShouldBe("orders");
        producer.GetTagItem("conveyo.message_type").ShouldBe("conveyo:test.example.v1");

        var properties = publisher.PublishedProperties.Single();
        properties.Headers.ShouldNotBeNull();
        properties.Headers!.ContainsKey("traceparent").ShouldBeTrue();
        var traceparent = Encoding.UTF8.GetString((byte[])properties.Headers["traceparent"]!);
        traceparent.ShouldContain(producer.TraceId.ToHexString());
    }

    [Test]
    public async Task Publish_EmitsProducerActivityWithPublishOperation()
    {
        // Arrange
        var publisher = TestChannel.Publisher();
        var endpoint = new RabbitMqPublishEndpoint(
            _ => Task.FromResult(publisher.Channel),
            exchangeName: "orders-exchange",
            hostInfo: new HostInfo(),
            urn: "conveyo:test.example.v1");

        // Act
        await endpoint.Publish(new ExampleMessage("hi"));

        // Assert
        var producer = _stoppedActivities.Single();
        producer.Kind.ShouldBe(ActivityKind.Producer);
        producer.OperationName.ShouldBe("orders-exchange publish");
        producer.GetTagItem("messaging.operation.type").ShouldBe("publish");
        producer.GetTagItem("messaging.destination.name").ShouldBe("orders-exchange");
    }

    [Test]
    public async Task Consumer_ExtractsParentContextFromHeaders()
    {
        // Arrange
        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
        var traceparent = $"00-{traceId.ToHexString()}-{spanId.ToHexString()}-01";

        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => Task.CompletedTask);

        // Act
        await handler.HandleMessageAsync(
            CreateDelivery(new ExampleMessage("hi"), headers: new Dictionary<string, object?>
            {
                ["traceparent"] = Encoding.UTF8.GetBytes(traceparent)
            }),
            "orders");

        // Assert
        var consumerActivity = _stoppedActivities.Single();
        consumerActivity.Kind.ShouldBe(ActivityKind.Consumer);
        consumerActivity.TraceId.ShouldBe(traceId);
        consumerActivity.ParentSpanId.ShouldBe(spanId);
        consumerActivity.Status.ShouldBe(ActivityStatusCode.Ok);
    }

    [Test]
    public async Task EndToEnd_PublisherAndConsumerShareTraceId()
    {
        // Arrange
        using var outerSource = new ActivitySource("OuterTest");
        using var outerListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "OuterTest",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(outerListener);

        using var outer = outerSource.StartActivity("outer", ActivityKind.Internal);
        outer.ShouldNotBeNull();
        var expectedTraceId = outer!.TraceId;

        var publisher = TestChannel.Publisher();
        var sendEndpoint = new RabbitMqSendEndpoint(
            _ => Task.FromResult(publisher.Channel),
            queueName: "orders",
            hostInfo: new HostInfo(),
            urn: "conveyo:test.example.v1");

        // Act
        await sendEndpoint.Send(new ExampleMessage("relay"));

        var publishedProperties = publisher.PublishedProperties.Single();

        var consumer = TestChannel.Consumer();
        var consumerPublisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(consumerPublisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => Task.CompletedTask);

        var sentBody = EnvelopeSerializer.Serialize(
            EnvelopeSerializer.Create(new ExampleMessage("relay"), new HostInfo(), "conveyo:test.example.v1"));

        var delivery = new BasicDeliverEventArgs(
            consumerTag: "consumer",
            deliveryTag: 100,
            redelivered: false,
            exchange: string.Empty,
            routingKey: "orders",
            properties: ToBasicProperties(publishedProperties),
            body: sentBody,
            cancellationToken: CancellationToken.None);

        await handler.HandleMessageAsync(delivery, "orders");

        // Assert
        var consumerActivity = _stoppedActivities.OfType<Activity>().Single(a => a.Kind == ActivityKind.Consumer);
        consumerActivity.TraceId.ShouldBe(expectedTraceId);
    }

    [Test]
    public async Task RetriesAddEventsAndFinalSuccessIsOk()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var attempts = 0;
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) =>
            {
                attempts++;
                if (attempts < 3)
                {
                    throw new InvalidOperationException("transient");
                }

                return Task.CompletedTask;
            },
            maxRetryCount: 3);

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("retry")), "orders");

        // Assert
        var activity = _stoppedActivities.Single();
        activity.Status.ShouldBe(ActivityStatusCode.Ok);
        var retryEvents = activity.Events.Where(e => e.Name == "retry").ToList();
        retryEvents.Count.ShouldBe(2);
    }

    [Test]
    public async Task ExhaustedRetriesSetActivityToError()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("permanent"),
            maxRetryCount: 0);

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("dead")), "orders");

        // Assert
        var activity = _stoppedActivities.Single();
        activity.Status.ShouldBe(ActivityStatusCode.Error);
        activity.StatusDescription.ShouldBe("permanent");
    }

    [Test]
    public async Task MessageNotConsumedKeepsStatusOk()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new MessageNotConsumedException("no consumer"));

        // Act
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("skip")), "orders");

        // Assert
        var activity = _stoppedActivities.Single();
        activity.Status.ShouldBe(ActivityStatusCode.Ok);
        activity.Events.Any(e => e.Name == "skipped").ShouldBeTrue();
    }

    [Test]
    public async Task MalformedEnvelopeStillEmitsErrorActivity()
    {
        // Arrange
        var consumer = TestChannel.Consumer();
        var publisher = TestChannel.Publisher();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => Task.CompletedTask);

        var delivery = new BasicDeliverEventArgs(
            consumerTag: "consumer",
            deliveryTag: 1,
            redelivered: false,
            exchange: string.Empty,
            routingKey: "orders",
            properties: new BasicProperties(),
            body: Encoding.UTF8.GetBytes("{not-json"),
            cancellationToken: CancellationToken.None);

        // Act
        await handler.HandleMessageAsync(delivery, "orders");

        // Assert
        var activity = _stoppedActivities.Single();
        activity.Status.ShouldBe(ActivityStatusCode.Error);
    }

    private static BasicDeliverEventArgs CreateDelivery<T>(T message, IDictionary<string, object?>? headers = null)
        where T : class
    {
        var body = EnvelopeSerializer.Serialize(
            EnvelopeSerializer.Create(message, new HostInfo(), "conveyo:test.example.v1"));
        var properties = new BasicProperties();
        if (headers is not null)
        {
            properties.Headers = headers;
        }
        return new BasicDeliverEventArgs(
            consumerTag: "consumer",
            deliveryTag: 42,
            redelivered: false,
            exchange: string.Empty,
            routingKey: "orders",
            properties: properties,
            body: body,
            cancellationToken: CancellationToken.None);
    }

    private static BasicProperties ToBasicProperties(IReadOnlyBasicProperties source)
    {
        var copy = new BasicProperties();
        if (source.Headers is not null)
        {
            copy.Headers = new Dictionary<string, object?>(source.Headers);
        }
        return copy;
    }

    private sealed record ExampleMessage(string Value);

}
