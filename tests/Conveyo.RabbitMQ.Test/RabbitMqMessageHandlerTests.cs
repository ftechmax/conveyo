using System.Reflection;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class RabbitMqMessageHandlerTests
{
    [Test]
    public void Constructor_RejectsNegativeRetryCount()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => _ = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => Task.CompletedTask,
            maxRetryCount: -1));

        Assert.That(ex!.ParamName, Is.EqualTo("maxRetryCount"));
    }

    [Test]
    public async Task HandleMessageAsync_MalformedEnvelopePublishesToErrorQueueAndAcks()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new AssertionException("Malformed envelopes should not reach consumers."));

        await handler.HandleMessageAsync(CreateDelivery("{not-json"), "orders");

        Assert.That(publisher.PublishedRoutingKeys, Is.EqualTo(new[] { "orders_error" }));
        Assert.That(publisher.PublishedMandatoryFlags, Is.EqualTo(new[] { true }));
        var properties = publisher.PublishedProperties.Single();
        Assert.That(properties.Headers, Is.Not.Null);
        Assert.That(properties.Headers!["conveyo-outcome"], Is.EqualTo("faulted"));
        Assert.That(properties.Headers["conveyo-fault-reason"], Is.EqualTo("deserialization-failed"));
        Assert.That(properties.Headers["conveyo-fault-original-queue"], Is.EqualTo("orders"));
        Assert.That(properties.Headers["conveyo-fault-attempts"], Is.EqualTo("1"));
        Assert.That(properties.Headers["conveyo-fault-exception-type"], Is.Not.Null);
        Assert.That(properties.Headers["conveyo-fault-exception-message"], Is.EqualTo(ExceptionInfo.RedactedMessage));
        Assert.That(properties.Headers.ContainsKey("conveyo-fault-stack-trace"), Is.False);
        Assert.That(consumer.AckedDeliveryTags, Is.EqualTo(new[] { 42UL }));
        Assert.That(consumer.NackedDeliveryTags, Is.Empty);
    }

    [Test]
    public async Task HandleMessageAsync_OversizedEnvelopePublishesRedactedMetadataToErrorQueueAndAcks()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new AssertionException("Oversized envelopes should not reach consumers."),
            maxEnvelopeSizeBytes: 4);

        await handler.HandleMessageAsync(CreateDelivery("12345"), "orders");

        Assert.That(publisher.PublishedRoutingKeys, Is.EqualTo(new[] { "orders_error" }));
        var properties = publisher.PublishedProperties.Single();
        Assert.That(properties.Headers, Is.Not.Null);
        Assert.That(properties.Headers!["conveyo-fault-reason"], Is.EqualTo("envelope-too-large"));
        Assert.That(properties.Headers["conveyo-fault-exception-message"], Is.EqualTo(ExceptionInfo.RedactedMessage));
        Assert.That(properties.Headers.ContainsKey("conveyo-fault-stack-trace"), Is.False);
        Assert.That(consumer.AckedDeliveryTags, Is.EqualTo(new[] { 42UL }));
        Assert.That(consumer.NackedDeliveryTags, Is.Empty);
    }

    [Test]
    public async Task HandleMessageAsync_ConsumerExceptionAfterRetriesPublishesToErrorQueueAndAcks()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            maxRetryCount: 0);

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders");

        Assert.That(publisher.DeclaredQueues, Is.EqualTo(new[] { "orders_error" }));
        Assert.That(publisher.Operations, Is.EqualTo(new[] { "declare:orders_error", "publish:orders_error" }));
        Assert.That(publisher.PublishedRoutingKeys, Is.EqualTo(new[] { "orders_error" }));
        Assert.That(publisher.PublishedMandatoryFlags, Is.EqualTo(new[] { true }));
        var properties = publisher.PublishedProperties.Single();
        Assert.That(properties.Headers, Is.Not.Null);
        Assert.That(properties.Headers!["conveyo-outcome"], Is.EqualTo("faulted"));
        Assert.That(properties.Headers["conveyo-fault-reason"], Is.EqualTo("exception"));
        Assert.That(properties.Headers["conveyo-fault-original-queue"], Is.EqualTo("orders"));
        Assert.That(properties.Headers["conveyo-fault-exception-type"], Is.EqualTo("System.InvalidOperationException"));
        Assert.That(properties.Headers["conveyo-fault-exception-message"], Is.EqualTo(ExceptionInfo.RedactedMessage));
        Assert.That(properties.Headers.ContainsKey("conveyo-fault-stack-trace"), Is.False);
        Assert.That(properties.Headers["conveyo-fault-attempts"], Is.EqualTo("1"));
        Assert.That(consumer.AckedDeliveryTags, Is.EqualTo(new[] { 42UL }));
        Assert.That(consumer.NackedDeliveryTags, Is.Empty);
    }

    [Test]
    public async Task HandleMessageAsync_IncludesExceptionDetailsInErrorHeadersWhenConfigured()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            maxRetryCount: 0,
            includeFaultExceptionDetails: true);

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders");

        var properties = publisher.PublishedProperties.Single();
        Assert.That(properties.Headers, Is.Not.Null);
        Assert.That(properties.Headers!["conveyo-fault-exception-type"], Is.EqualTo("System.InvalidOperationException"));
        Assert.That(properties.Headers["conveyo-fault-exception-message"], Is.EqualTo("consumer failed"));
        Assert.That(properties.Headers.ContainsKey("conveyo-fault-stack-trace"), Is.True);
    }

    [Test]
    public async Task HandleMessageAsync_InvokesFaultHookWithAccumulatedExceptionsBeforePublishingToErrorQueue()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var attempt = 0;
        MessageEnvelope? observedEnvelope = null;
        IReadOnlyList<Exception>? observedExceptions = null;

        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) =>
            {
                attempt++;
                throw new InvalidOperationException($"failure #{attempt}");
            },
            onFaultAsync: (envelope, exceptions, _) =>
            {
                observedEnvelope = envelope;
                observedExceptions = exceptions.ToArray();
                Assert.That(publisher.PublishedRoutingKeys, Is.Empty, "fault hook must run before publishing to the error queue");
                Assert.That(consumer.AckedDeliveryTags, Is.Empty, "fault hook must run before ack");
                return Task.CompletedTask;
            },
            maxRetryCount: 0);

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders");

        Assert.That(observedEnvelope, Is.Not.Null);
        Assert.That(observedExceptions, Is.Not.Null);
        Assert.That(observedExceptions!.Select(ex => ex.Message), Is.EqualTo(new[] { "failure #1" }));
        Assert.That(publisher.PublishedRoutingKeys, Is.EqualTo(new[] { "orders_error" }));
        Assert.That(consumer.AckedDeliveryTags, Is.EqualTo(new[] { 42UL }));
    }

    [Test]
    public async Task HandleMessageAsync_FaultHookExceptionDoesNotBlockErrorPublishOrAck()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            onFaultAsync: (_, _, _) => throw new InvalidOperationException("fault publish failed"),
            maxRetryCount: 0);

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders");

        Assert.That(publisher.PublishedRoutingKeys, Is.EqualTo(new[] { "orders_error" }));
        Assert.That(consumer.AckedDeliveryTags, Is.EqualTo(new[] { 42UL }));
        Assert.That(consumer.NackedDeliveryTags, Is.Empty);
    }

    [Test]
    public async Task HandleMessageAsync_FaultHookNotInvokedForSkippedMessages()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var faultInvoked = false;
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new MessageNotConsumedException("no consumer for type"),
            onFaultAsync: (_, _, _) =>
            {
                faultInvoked = true;
                return Task.CompletedTask;
            });

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("hello")), "orders");

        Assert.That(faultInvoked, Is.False);
    }

    [Test]
    public void HandleMessageAsync_DoesNotAckWhenErrorPublishFails()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        publisher.PublishException = new InvalidOperationException("error publish failed");
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            maxRetryCount: 0);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("boom")), "orders"));

        Assert.That(ex!.Message, Is.EqualTo(ErrorMessages.TerminalQueuePublishFailed("orders_error")));
        Assert.That(ex.InnerException, Is.TypeOf<InvalidOperationException>());
        Assert.That(ex.InnerException!.Message, Is.EqualTo("error publish failed"));
        Assert.That(consumer.AckedDeliveryTags, Is.Empty);
        Assert.That(consumer.NackedDeliveryTags, Is.Empty);
    }

    [Test]
    public async Task HandleMessageAsync_SkippedMessagePublishesToSkippedQueueWithDiscriminatorHeader()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new MessageNotConsumedException("no consumer for type"));

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("hello")), "orders");

        Assert.That(publisher.DeclaredQueues, Is.EqualTo(new[] { "orders_skipped" }));
        Assert.That(publisher.Operations, Is.EqualTo(new[] { "declare:orders_skipped", "publish:orders_skipped" }));
        Assert.That(publisher.PublishedRoutingKeys, Is.EqualTo(new[] { "orders_skipped" }));
        Assert.That(publisher.PublishedMandatoryFlags, Is.EqualTo(new[] { true }));
        var properties = publisher.PublishedProperties.Single();
        Assert.That(properties.Persistent, Is.True);
        Assert.That(properties.Headers, Is.Not.Null);
        Assert.That(properties.Headers!["conveyo-outcome"], Is.EqualTo("skipped"));
        Assert.That(properties.Headers["conveyo-skipped-reason"], Is.EqualTo("no consumer for type"));
        Assert.That(properties.Headers["conveyo-skipped-original-queue"], Is.EqualTo("orders"));
        Assert.That(consumer.AckedDeliveryTags, Is.EqualTo(new[] { 42UL }));
        Assert.That(consumer.NackedDeliveryTags, Is.Empty);
    }

    [Test]
    public async Task HandleMessageAsync_DeclaresTerminalQueueOnlyOncePerHandler()
    {
        var consumer = TestChannel.Create();
        var publisher = TestChannel.Create();
        var handler = new RabbitMqMessageHandler(
            consumer.Channel,
            _ => Task.FromResult(publisher.Channel),
            logger: null,
            onMessageAsync: (_, _) => throw new InvalidOperationException("consumer failed"),
            maxRetryCount: 0);

        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("first")), "orders");
        await handler.HandleMessageAsync(CreateDelivery(new ExampleMessage("second")), "orders");

        Assert.That(publisher.DeclaredQueues, Is.EqualTo(new[] { "orders_error" }));
        Assert.That(publisher.PublishedRoutingKeys, Is.EqualTo(new[] { "orders_error", "orders_error" }));
    }

    private static BasicDeliverEventArgs CreateDelivery(string body)
    {
        return new BasicDeliverEventArgs(
            consumerTag: "consumer",
            deliveryTag: 42,
            redelivered: false,
            exchange: string.Empty,
            routingKey: "orders",
            properties: new BasicProperties(),
            body: Encoding.UTF8.GetBytes(body),
            cancellationToken: CancellationToken.None);
    }

    private static BasicDeliverEventArgs CreateDelivery<T>(T message)
        where T : class
    {
        var body = EnvelopeSerializer.Serialize(EnvelopeSerializer.Create(message, new HostInfo(), "conveyo:test.example.v1"));
        return new BasicDeliverEventArgs(
            consumerTag: "consumer",
            deliveryTag: 42,
            redelivered: false,
            exchange: string.Empty,
            routingKey: "orders",
            properties: new BasicProperties(),
            body: body,
            cancellationToken: CancellationToken.None);
    }

    private sealed record ExampleMessage(string Value);

    private class TestChannel : DispatchProxy
    {
        public IChannel Channel { get; private set; } = null!;

        public Exception? PublishException { get; set; }

        public Exception? NackException { get; set; }

        public List<string> PublishedRoutingKeys { get; } = [];

        public List<IReadOnlyBasicProperties> PublishedProperties { get; } = [];

        public List<bool> PublishedMandatoryFlags { get; } = [];

        public List<string> DeclaredQueues { get; } = [];

        public List<string> Operations { get; } = [];

        public List<ulong> AckedDeliveryTags { get; } = [];

        public List<ulong> NackedDeliveryTags { get; } = [];

        public List<bool> NackedRequeueFlags { get; } = [];

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
                if (PublishException != null)
                {
                    throw PublishException;
                }

                PublishedRoutingKeys.Add((string)args![1]!);
                Operations.Add($"publish:{args[1]}");
                PublishedMandatoryFlags.Add((bool)args[2]!);
                PublishedProperties.Add((IReadOnlyBasicProperties)args[3]!);
                return ValueTask.CompletedTask;
            }

            if (targetMethod.Name == nameof(IChannel.QueueDeclareAsync))
            {
                DeclaredQueues.Add((string)args![0]!);
                Operations.Add($"declare:{args[0]}");
                return CompletedTask(targetMethod.ReturnType);
            }

            if (targetMethod.Name == nameof(IChannel.BasicAckAsync))
            {
                AckedDeliveryTags.Add((ulong)args![0]!);
                return ValueTask.CompletedTask;
            }

            if (targetMethod.Name == nameof(IChannel.BasicNackAsync))
            {
                if (NackException != null)
                {
                    throw NackException;
                }

                NackedDeliveryTags.Add((ulong)args![0]!);
                NackedRequeueFlags.Add((bool)args[2]!);
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
