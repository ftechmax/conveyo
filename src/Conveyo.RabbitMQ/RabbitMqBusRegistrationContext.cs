using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqBusRegistrationContext(ConveyoRegistration registration) : IRabbitMqBusRegistrationContext, IBusRegistrationContext
{
    private ConveyoContext Context => registration.Context;

    // The built context is immutable, so the groupings are derived once. Nothing is cached while
    // registration is still open, because Context throws until it completes.
    private ILookup<string, Type> Queues => _queues ??= Context.ConsumerEndpoints
        .SelectMany(pair => pair.Value.Select(address => (Queue: QueueAddress.GetQueueName(address), Consumer: pair.Key)))
        .ToLookup(endpoint => endpoint.Queue, endpoint => endpoint.Consumer, StringComparer.Ordinal);

    private ILookup<string, Type>? _queues;
    private RabbitMqHostOptions? _hostOptions;
    private ILogger? _logger;
    private CancellationTokenSource? _stopping;
    private RabbitMqConnectionManager? _connectionManager;

    public IConnection? Connection => _connectionManager?.Connection;

    public IChannel? Channel => _connectionManager?.ConsumerChannel;

    event Func<MessageEnvelope, CancellationToken, Task>? IBusRegistrationContext.OnMessageAsync
    {
        add => OnMessageAsync += value;
        remove => OnMessageAsync -= value;
    }

    event Func<MessageEnvelope, IReadOnlyList<Exception>, CancellationToken, Task>? IBusRegistrationContext.OnFaultAsync
    {
        add => OnFaultAsync += value;
        remove => OnFaultAsync -= value;
    }

    private event Func<MessageEnvelope, CancellationToken, Task>? OnMessageAsync;

    private event Func<MessageEnvelope, IReadOnlyList<Exception>, CancellationToken, Task>? OnFaultAsync;

    public void SetOptions(RabbitMqHostOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _hostOptions = options with { };
    }

    public void SetLogger(ILogger<RabbitMqBusRegistrationContext> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);

        _logger = logger;
    }

    public void RegisterConsumer<T>(string queueName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        registration.RegisterConsumerEndpoint(typeof(T), QueueAddress.Create(queueName));
    }

    public async Task StartAsync(ConveyoContext context, CancellationToken cancellationToken)
    {
        var hostOptions = _hostOptions
            ?? throw new InvalidOperationException(ErrorMessages.HostNotConfigured);

        if (_stopping is not null)
        {
            throw new InvalidOperationException(ErrorMessages.BusAlreadyStarted);
        }
        _stopping = new CancellationTokenSource();
        try
        {
            _connectionManager = new RabbitMqConnectionManager(_logger);
            await _connectionManager.StartAsync(hostOptions, cancellationToken);

            var consumerChannel = _connectionManager.ConsumerChannel
                ?? throw new InvalidOperationException(ErrorMessages.ChannelNotInitialized);

            var messageHandler = CreateMessageHandler(hostOptions, consumerChannel);
            var declaredExchanges = new HashSet<string>(StringComparer.Ordinal);

            // Declare all topology before starting consumers, so an early delivery can't publish to a
            // not-yet-declared exchange. Declares are idempotent.
            foreach (var queue in Queues)
            {
                await DeclareConsumerTopologyAsync(consumerChannel, queue.Key, queue.ToArray(), context, declaredExchanges, cancellationToken);
            }

            await DeclareProducerExchangesAsync(consumerChannel, context, declaredExchanges, cancellationToken);

            foreach (var queue in Queues)
            {
                await StartConsumerAsync(consumerChannel, queue.Key, messageHandler, cancellationToken);
            }
        }
        catch
        {
            await StopAsync(CancellationToken.None);
            throw;
        }
    }

    internal static async Task DeclareProducerExchangesAsync(
        IChannel channel,
        ConveyoContext options,
        HashSet<string> alreadyDeclared,
        CancellationToken cancellationToken)
    {
        foreach (var (messageType, urn) in options.UrnsByType)
        {
            // Fault<T> exchanges are declared lazily on first publish
            if (IsFaultType(messageType) || !alreadyDeclared.Add(urn))
            {
                continue;
            }

            await RabbitMqTopology.DeclareDurableFanoutExchangeAsync(channel, urn, cancellationToken);
        }
    }

    private static bool IsFaultType(Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Fault<>);

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_stopping is not null)
            {
                await _stopping.CancelAsync();
            }
        }
        finally
        {
            try
            {
                if (_connectionManager is not null)
                {
                    await _connectionManager.StopAsync(cancellationToken);
                }
            }
            finally
            {
                _connectionManager = null;
                _stopping?.Dispose();
                _stopping = null;
            }
        }
    }

    public string GetQueueName(Type messageType)
    {
        if (Context.EndpointConventions.TryGetValue(messageType, out var conventionAddress))
        {
            return QueueAddress.GetQueueName(conventionAddress);
        }

        var handlerTypes = Context.ConsumerMessages
            .Where(registeredConsumer => registeredConsumer.Value.Contains(messageType))
            .Select(registeredConsumer => registeredConsumer.Key)
            .ToList();
        if (handlerTypes.Count == 0)
        {
            throw new InvalidOperationException(ErrorMessages.NoHandlerFoundForMessageType(messageType));
        }

        // More than one matching queue means Send has no unambiguous target — fail fast rather than
        // route to whichever happened to be registered first.
        var queueNames = Queues
            .Where(queue => queue.Any(handlerTypes.Contains))
            .Select(queue => queue.Key)
            .ToList();
        if (queueNames.Count == 0)
        {
            throw new InvalidOperationException(ErrorMessages.NoQueueFoundForHandlerType(handlerTypes[0]));
        }
        if (queueNames.Count > 1)
        {
            throw new InvalidOperationException(ErrorMessages.AmbiguousSendTarget(messageType, queueNames));
        }

        return queueNames[0];
    }

    public string GetExchangeName(Type type)
        => Context.UrnFor(type);

    internal Task<IChannel> CreatePublisherChannelAsync(CancellationToken cancellationToken)
    {
        var connectionManager = _connectionManager
            ?? throw new InvalidOperationException(ErrorMessages.ConnectionNotInitialized);

        return connectionManager.CreatePublisherChannelAsync(cancellationToken);
    }

    private RabbitMqMessageHandler CreateMessageHandler(RabbitMqHostOptions hostOptions, IChannel consumerChannel)
        => new(
            consumerChannel,
            CreatePublisherChannelAsync,
            _logger,
            (envelope, ct) => OnMessageAsync?.Invoke(envelope, ct) ?? Task.CompletedTask,
            (envelope, exceptions, ct) => OnFaultAsync?.Invoke(envelope, exceptions, ct) ?? Task.CompletedTask,
            hostOptions.MaxRetryCount,
            hostOptions.MaxEnvelopeSizeBytes,
            hostOptions.IncludeFaultExceptionDetails,
            _stopping!.Token);

    private async Task DeclareConsumerTopologyAsync(
        IChannel channel,
        string queueName,
        IReadOnlyCollection<Type> consumerTypes,
        ConveyoContext options,
        HashSet<string> declaredExchanges,
        CancellationToken cancellationToken)
    {
        await RabbitMqTopology.DeclareDurableQueueAsync(channel, queueName, cancellationToken);
        await RabbitMqTopology.DeclareDurableFanoutExchangeAsync(channel, queueName, cancellationToken);
        await channel.QueueBindAsync(
            queue: queueName,
            exchange: queueName,
            routingKey: string.Empty,
            cancellationToken: cancellationToken);

        declaredExchanges.Add(queueName);

        foreach (var consumerType in consumerTypes)
        {
            foreach (var consumerMessage in options.ConsumerMessages[consumerType])
            {
                var messageExchange = GetExchangeName(consumerMessage);
                if (declaredExchanges.Add(messageExchange))
                {
                    await RabbitMqTopology.DeclareDurableFanoutExchangeAsync(channel, messageExchange, cancellationToken);
                }

                await channel.ExchangeBindAsync(
                    destination: queueName,
                    source: messageExchange,
                    routingKey: string.Empty,
                    arguments: null,
                    cancellationToken: cancellationToken);
            }
        }
    }

    private static async Task StartConsumerAsync(
        IChannel channel,
        string queueName,
        RabbitMqMessageHandler messageHandler,
        CancellationToken cancellationToken)
    {
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, @event) => messageHandler.HandleMessageAsync(@event, queueName);

        await channel.BasicConsumeAsync(queueName, autoAck: false, consumer, cancellationToken);
    }

}
