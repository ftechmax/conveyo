using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqBusRegistrationContext(ConveyoContext conveyoOptions) : IRabbitMqBusRegistrationContext, IBusRegistrationContext
{
    private readonly Dictionary<string, List<Type>> _consumers = new(StringComparer.Ordinal);

    private RabbitMqHostOptions? _hostOptions;
    private ILogger? _logger;
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

        _hostOptions = options;
    }

    public void SetLogger(ILogger<RabbitMqBusRegistrationContext> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);

        _logger = logger;
    }

    public void RegisterConsumer<T>(string queueName) where T : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        if (!_consumers.TryGetValue(queueName, out var consumerTypes))
        {
            consumerTypes = [];
            _consumers[queueName] = consumerTypes;
        }

        consumerTypes.Add(typeof(T));

        if (!conveyoOptions._consumerEndpoints.TryGetValue(typeof(T), out var endpoints))
        {
            endpoints = [];
            conveyoOptions._consumerEndpoints[typeof(T)] = endpoints;
        }

        var endpointAddress = QueueAddress.Create(queueName);
        if (!endpoints.Contains(endpointAddress))
        {
            endpoints.Add(endpointAddress);
        }
    }

    public async Task StartAsync(ConveyoContext context, CancellationToken cancellationToken)
    {
        var hostOptions = _hostOptions
            ?? throw new InvalidOperationException(ErrorMessages.HostNotConfigured);

        _connectionManager = new RabbitMqConnectionManager(_logger);
        await _connectionManager.StartAsync(hostOptions, cancellationToken);

        var consumerChannel = _connectionManager.ConsumerChannel
            ?? throw new InvalidOperationException(ErrorMessages.ChannelNotInitialized);

        var messageHandler = CreateMessageHandler(hostOptions, consumerChannel);
        var declaredExchanges = new HashSet<string>(StringComparer.Ordinal);

        // Declare all topology before starting consumers, so an early delivery can't publish to a
        // not-yet-declared exchange. Declares are idempotent.
        foreach (var (queueName, consumerTypes) in _consumers)
        {
            await DeclareConsumerTopologyAsync(consumerChannel, queueName, consumerTypes, context, declaredExchanges, cancellationToken);
        }

        await DeclareProducerExchangesAsync(consumerChannel, context, declaredExchanges, cancellationToken);

        foreach (var queueName in _consumers.Keys)
        {
            await StartConsumerAsync(consumerChannel, queueName, messageHandler, cancellationToken);
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
        if (_connectionManager != null)
        {
            await _connectionManager.StopAsync(cancellationToken);
        }
    }

    public string GetQueueName(Type messageType)
    {
        if (conveyoOptions.EndpointConventions.TryGetValue(messageType, out var conventionAddress))
        {
            return QueueAddress.GetQueueName(conventionAddress);
        }

        var handlerTypes = conveyoOptions.ConsumerMessages
            .Where(registeredConsumer => registeredConsumer.Value.Contains(messageType))
            .Select(registeredConsumer => registeredConsumer.Key)
            .ToList();
        if (handlerTypes.Count == 0)
        {
            throw new InvalidOperationException(ErrorMessages.NoHandlerFoundForMessageType(messageType));
        }

        // More than one matching queue means Send has no unambiguous target — fail fast rather than
        // route to whichever happened to be registered first.
        var queueNames = _consumers
            .Where(registeredQueue => registeredQueue.Value.Any(handlerTypes.Contains))
            .Select(registeredQueue => registeredQueue.Key)
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
        => conveyoOptions.UrnFor(type);

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
            hostOptions.IncludeFaultExceptionDetails);

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
