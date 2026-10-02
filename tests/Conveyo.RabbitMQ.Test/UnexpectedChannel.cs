using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Conveyo.RabbitMQ.Test;

// Test channels override only explicitly supported operations. Every other operation fails.
internal abstract class UnexpectedChannel : IChannel
{
    public virtual int ChannelNumber => throw new NotSupportedException(nameof(ChannelNumber));
    public virtual ShutdownEventArgs? CloseReason => throw new NotSupportedException(nameof(CloseReason));
    public virtual IAsyncBasicConsumer? DefaultConsumer
    {
        get => throw new NotSupportedException(nameof(DefaultConsumer));
        set => throw new NotSupportedException(nameof(DefaultConsumer));
    }
    public virtual bool IsClosed => throw new NotSupportedException(nameof(IsClosed));
    public virtual bool IsOpen => throw new NotSupportedException(nameof(IsOpen));
    public virtual string? CurrentQueue => throw new NotSupportedException(nameof(CurrentQueue));
    public virtual TimeSpan ContinuationTimeout
    {
        get => throw new NotSupportedException(nameof(ContinuationTimeout));
        set => throw new NotSupportedException(nameof(ContinuationTimeout));
    }
    public virtual event AsyncEventHandler<BasicAckEventArgs> BasicAcksAsync
    {
        add => throw new NotSupportedException(nameof(BasicAcksAsync));
        remove => throw new NotSupportedException(nameof(BasicAcksAsync));
    }
    public virtual event AsyncEventHandler<BasicNackEventArgs> BasicNacksAsync
    {
        add => throw new NotSupportedException(nameof(BasicNacksAsync));
        remove => throw new NotSupportedException(nameof(BasicNacksAsync));
    }
    public virtual event AsyncEventHandler<BasicReturnEventArgs> BasicReturnAsync
    {
        add => throw new NotSupportedException(nameof(BasicReturnAsync));
        remove => throw new NotSupportedException(nameof(BasicReturnAsync));
    }
    public virtual event AsyncEventHandler<CallbackExceptionEventArgs> CallbackExceptionAsync
    {
        add => throw new NotSupportedException(nameof(CallbackExceptionAsync));
        remove => throw new NotSupportedException(nameof(CallbackExceptionAsync));
    }
    public virtual event AsyncEventHandler<FlowControlEventArgs> FlowControlAsync
    {
        add => throw new NotSupportedException(nameof(FlowControlAsync));
        remove => throw new NotSupportedException(nameof(FlowControlAsync));
    }
    public virtual event AsyncEventHandler<ShutdownEventArgs> ChannelShutdownAsync
    {
        add => throw new NotSupportedException(nameof(ChannelShutdownAsync));
        remove => throw new NotSupportedException(nameof(ChannelShutdownAsync));
    }
    public virtual ValueTask<ulong> GetNextPublishSequenceNumberAsync(
        CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(GetNextPublishSequenceNumberAsync));
    public virtual ValueTask BasicAckAsync(
        ulong deliveryTag, bool multiple, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(BasicAckAsync));
    public virtual ValueTask BasicNackAsync(
        ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(BasicNackAsync));
    public virtual Task BasicCancelAsync(
        string consumerTag, bool noWait, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(BasicCancelAsync));
    public virtual Task<string> BasicConsumeAsync(
        string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive,
        IDictionary<string, object?>? arguments, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(BasicConsumeAsync));
    public virtual Task<BasicGetResult?> BasicGetAsync(
        string queue, bool autoAck, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(BasicGetAsync));
    public virtual ValueTask BasicPublishAsync<TProperties>(
        string exchange, string routingKey, bool mandatory, TProperties basicProperties, ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken) where TProperties : IReadOnlyBasicProperties, IAmqpHeader
        => throw new NotSupportedException(nameof(BasicPublishAsync));
    public virtual ValueTask BasicPublishAsync<TProperties>(
        CachedString exchange, CachedString routingKey, bool mandatory, TProperties basicProperties,
        ReadOnlyMemory<byte> body, CancellationToken cancellationToken) where TProperties : IReadOnlyBasicProperties, IAmqpHeader
        => throw new NotSupportedException(nameof(BasicPublishAsync));
    public virtual Task BasicQosAsync(
        uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(BasicQosAsync));
    public virtual ValueTask BasicRejectAsync(
        ulong deliveryTag, bool requeue, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(BasicRejectAsync));
    public virtual Task CloseAsync(
        ushort replyCode, string replyText, bool abort, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(CloseAsync));
    public virtual Task CloseAsync(
        ShutdownEventArgs reason, bool abort)
        => throw new NotSupportedException(nameof(CloseAsync));
    public virtual Task CloseAsync(
        ShutdownEventArgs reason, bool abort, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(CloseAsync));
    public virtual Task ExchangeDeclareAsync(
        string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object?>? arguments,
        bool passive, bool noWait, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(ExchangeDeclareAsync));
    public virtual Task ExchangeDeclarePassiveAsync(
        string exchange, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(ExchangeDeclarePassiveAsync));
    public virtual Task ExchangeDeleteAsync(
        string exchange, bool ifUnused, bool noWait, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(ExchangeDeleteAsync));
    public virtual Task ExchangeBindAsync(
        string destination, string source, string routingKey, IDictionary<string, object?>? arguments, bool noWait,
        CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(ExchangeBindAsync));
    public virtual Task ExchangeUnbindAsync(
        string destination, string source, string routingKey, IDictionary<string, object?>? arguments, bool noWait,
        CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(ExchangeUnbindAsync));
    public virtual Task<QueueDeclareOk> QueueDeclareAsync(
        string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments,
        bool passive, bool noWait, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(QueueDeclareAsync));
    public virtual Task<QueueDeclareOk> QueueDeclarePassiveAsync(
        string queue, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(QueueDeclarePassiveAsync));
    public virtual Task<uint> QueueDeleteAsync(
        string queue, bool ifUnused, bool ifEmpty, bool noWait, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(QueueDeleteAsync));
    public virtual Task<uint> QueuePurgeAsync(
        string queue, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(QueuePurgeAsync));
    public virtual Task QueueBindAsync(
        string queue, string exchange, string routingKey, IDictionary<string, object?>? arguments, bool noWait,
        CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(QueueBindAsync));
    public virtual Task QueueUnbindAsync(
        string queue, string exchange, string routingKey, IDictionary<string, object?>? arguments,
        CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(QueueUnbindAsync));
    public virtual Task<uint> MessageCountAsync(
        string queue, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(MessageCountAsync));
    public virtual Task<uint> ConsumerCountAsync(
        string queue, CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(ConsumerCountAsync));
    public virtual Task TxCommitAsync(
        CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(TxCommitAsync));
    public virtual Task TxRollbackAsync(
        CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(TxRollbackAsync));
    public virtual Task TxSelectAsync(
        CancellationToken cancellationToken)
        => throw new NotSupportedException(nameof(TxSelectAsync));
    public virtual void Dispose() => throw new NotSupportedException(nameof(Dispose));
    public virtual ValueTask DisposeAsync() => throw new NotSupportedException(nameof(DisposeAsync));
}
