using System.Diagnostics;
using System.Reflection;
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
        var publisher = TestChannel.Create();
        var endpoint = new RabbitMqSendEndpoint(
            _ => Task.FromResult(publisher.Channel),
            queueName: "orders",
            hostInfo: new HostInfo(),
            urn: "conveyo:test.example.v1");

        await endpoint.Send(new ExampleMessage("hello"));

        var producer = _stoppedActivities.Single();
        Assert.That(producer.Kind, Is.EqualTo(ActivityKind.Producer));
        Assert.That(producer.OperationName, Is.EqualTo("orders send"));
        Assert.That(producer.GetTagItem("messaging.system"), Is.EqualTo(RabbitMqDiagnosticHeaders.MessagingSystem));
        Assert.That(producer.GetTagItem("messaging.operation.type"), Is.EqualTo("send"));
        Assert.That(producer.GetTagItem("messaging.destination.name"), Is.EqualTo("orders"));
        Assert.That(producer.GetTagItem(RabbitMqDiagnosticHeaders.RoutingKey), Is.EqualTo("orders"));
        Assert.That(producer.GetTagItem("conveyo.message_type"), Is.EqualTo("conveyo:test.example.v1"));

        var properties = publisher.PublishedProperties.Single();
        Assert.That(properties.Headers, Is.Not.Null);
        Assert.That(properties.Headers!.ContainsKey("traceparent"), Is.True);
        var traceparent = Encoding.UTF8.GetString((byte[])properties.Headers["traceparent"]!);
        Assert.That(traceparent, Does.Contain(producer.TraceId.ToHexString()));
    }

    [Test]
    public async Task Publish_EmitsProducerActivityWithPublishOperation()
    {
        var publisher = TestChannel.Create();
        var endpoint = new RabbitMqPublishEndpoint(
            _ => Task.FromResult(publisher.Channel),
            exchangeName: "orders-exchange",
            hostInfo: new HostInfo(),
            urn: "conveyo:test.example.v1");

        await endpoint.Publish(new ExampleMessage("hi"));

        var producer = _stoppedActivities.Single();
        Assert.That(producer.Kind, Is.EqualTo(ActivityKind.Producer));
        Assert.That(producer.OperationName, Is.EqualTo("orders-exchange publish"));
        Assert.That(producer.GetTagItem("messaging.operation.type"), Is.EqualTo("publish"));
        Assert.That(producer.GetTagItem("messaging.destination.name"), Is.EqualTo("orders-exchange"));
    }

    [Test]
    public async Task Consumer_ExtractsParentContextFromHeaders()
    {
        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
        var traceparent = $"00-{traceId.ToHexString()}-{spanId.ToHexString()}-01";

        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => Task.CompletedTask);

        await handler.HandleMessageAsync(
            CreateDelivery(new ExampleMessage("hi"), headers: new Dictionary<string, object?>
            {
                ["traceparent"] = Encoding.UTF8.GetBytes(traceparent)
            }),
            "orders");

        var consumerActivity = _stoppedActivities.Single();
        Assert.That(consumerActivity.Kind, Is.EqualTo(ActivityKind.Consumer));
        Assert.That(consumerActivity.TraceId, Is.EqualTo(traceId));
        Assert.That(consumerActivity.ParentSpanId, Is.EqualTo(spanId));
        Assert.That(consumerActivity.Status, Is.EqualTo(ActivityStatusCode.Ok));
    }

    [Test]
    public async Task EndToEnd_PublisherAndConsumerShareTraceId()
    {
        using var outerSource = new ActivitySource("OuterTest");
        using var outerListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "OuterTest",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(outerListener);

        using var outer = outerSource.StartActivity("outer", ActivityKind.Internal);
        Assert.That(outer, Is.Not.Null);
        var expectedTraceId = outer!.TraceId;

        var publisher = TestChannel.Create();
        var sendEndpoint = new RabbitMqSendEndpoint(
            _ => Task.FromResult(publisher.Channel),
            queueName: "orders",
            hostInfo: new HostInfo(),
            urn: "conveyo:test.example.v1");

        await sendEndpoint.Send(new ExampleMessage("relay"));

        var publishedProperties = publisher.PublishedProperties.Single();

        var consumer = TestChannel.Create();
        var consumerPublisher = TestChannel.Create();
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

        var consumerActivity = _stoppedActivities.OfType<Activity>().Single(a => a.Kind == ActivityKind.Consumer);
        Assert.That(consumerActivity.TraceId, Is.EqualTo(expectedTraceId));
    }

    [Test]
    public async Task RetriesAddEventsAndFinalSuccessIsOk()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
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

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("retry")), "orders");

        var activity = _stoppedActivities.Single();
        Assert.That(activity.Status, Is.EqualTo(ActivityStatusCode.Ok));
        var retryEvents = activity.Events.Where(e => e.Name == "retry").ToList();
        Assert.That(retryEvents, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task ExhaustedRetriesSetActivityToError()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("permanent"),
            maxRetryCount: 0);

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("dead")), "orders");

        var activity = _stoppedActivities.Single();
        Assert.That(activity.Status, Is.EqualTo(ActivityStatusCode.Error));
        Assert.That(activity.StatusDescription, Is.EqualTo("permanent"));
    }

    [Test]
    public async Task MessageNotConsumedKeepsStatusOk()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new MessageNotConsumedException("no consumer"));

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("skip")), "orders");

        var activity = _stoppedActivities.Single();
        Assert.That(activity.Status, Is.EqualTo(ActivityStatusCode.Ok));
        Assert.That(activity.Events.Any(e => e.Name == "skipped"), Is.True);
    }

    [Test]
    public async Task MalformedEnvelopeStillEmitsErrorActivity()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
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

        await handler.HandleMessageAsync(delivery, "orders");

        var activity = _stoppedActivities.Single();
        Assert.That(activity.Status, Is.EqualTo(ActivityStatusCode.Error));
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

    private class TestChannel : DispatchProxy
    {
        public IChannel Channel { get; private set; } = null!;

        public List<IReadOnlyBasicProperties> PublishedProperties { get; } = [];

        public static TestChannel Create()
        {
            var channel = Create<IChannel, TestChannel>();
            var proxy = (TestChannel)(object)channel!;
            proxy.Channel = channel;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod == null)
            {
                return null;
            }

            if (targetMethod.Name == nameof(IChannel.BasicPublishAsync))
            {
                PublishedProperties.Add((IReadOnlyBasicProperties)args![3]!);
                return ValueTask.CompletedTask;
            }

            if (targetMethod.Name == nameof(IChannel.QueueDeclareAsync))
            {
                return CompletedTask(targetMethod.ReturnType);
            }

            if (targetMethod.Name == nameof(IChannel.BasicAckAsync) ||
                targetMethod.Name == nameof(IChannel.BasicNackAsync))
            {
                return ValueTask.CompletedTask;
            }

            if (targetMethod.Name == nameof(IAsyncDisposable.DisposeAsync))
            {
                return ValueTask.CompletedTask;
            }

            if (targetMethod.Name == nameof(IDisposable.Dispose))
            {
                return null;
            }

            if (targetMethod.ReturnType == typeof(bool))
            {
                return true;
            }

            if (targetMethod.ReturnType == typeof(string))
            {
                return string.Empty;
            }

            if (targetMethod.ReturnType == typeof(ValueTask))
            {
                return ValueTask.CompletedTask;
            }

            if (targetMethod.ReturnType == typeof(Task))
            {
                return Task.CompletedTask;
            }

            if (targetMethod.ReturnType.IsGenericType &&
                targetMethod.ReturnType.GetGenericTypeDefinition() == typeof(Task<>))
            {
                return CompletedTask(targetMethod.ReturnType);
            }

            if (targetMethod.ReturnType == typeof(ushort))
            {
                return (ushort)1;
            }

            return null;
        }

        private static object CompletedTask(Type returnType)
        {
            if (returnType == typeof(Task))
            {
                return Task.CompletedTask;
            }

            var resultType = returnType.GetGenericArguments()[0];
            var method = typeof(TestChannel)
                .GetMethod(nameof(TaskFromDefault), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(resultType);

            return method.Invoke(null, null)!;
        }

        private static Task<T> TaskFromDefault<T>() => Task.FromResult(default(T)!);
    }
}
