using RabbitMQ.Client;

namespace Conveyo.RabbitMQ.Test;

internal sealed class TestChannel : UnexpectedChannel
{
    private enum Role { Consumer, Publisher, Topology }
    private readonly Role _role;
    private TestChannel(Role role) => _role = role;
    public static TestChannel Consumer() => new(Role.Consumer);
    public static TestChannel Publisher() => new(Role.Publisher);
    public static TestChannel Topology() => new(Role.Topology);
    public IChannel Channel => this;

    public Exception? PublishException { get; set; }
    public Exception? DeclareException { get; set; }
    public Exception? AckException { get; set; }
    public Func<CancellationToken, Task>? BeforePublishAsync { get; set; }
    public Action? BeforeAck { get; set; }
    public int DisposeCount { get; private set; }
    public List<Publication> Publications { get; } = [];
    public List<Declaration> Declarations { get; } = [];
    public List<string> DeclaredQueues { get; } = [];
    public List<string> Operations { get; } = [];
    public List<ulong> AckedDeliveryTags { get; } = [];
    public IEnumerable<string> PublishedRoutingKeys => Publications.Select(p => p.RoutingKey);
    public IEnumerable<IReadOnlyBasicProperties> PublishedProperties => Publications.Select(p => p.Properties);
    public IEnumerable<bool> PublishedMandatoryFlags => Publications.Select(p => p.Mandatory);
    public IEnumerable<string> DeclaredExchanges => Declarations.Select(d => d.Exchange);

    public sealed record Publication(string Exchange, string RoutingKey, bool Mandatory, IReadOnlyBasicProperties Properties, byte[] Body);
    public sealed record Declaration(string Exchange, string Type, bool Durable, bool AutoDelete);

    public override async ValueTask BasicPublishAsync<TProperties>(string exchange, string routingKey, bool mandatory,
        TProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
    {
        Require(Role.Publisher);
        cancellationToken.ThrowIfCancellationRequested();
        if (BeforePublishAsync is not null) await BeforePublishAsync(cancellationToken);
        if (PublishException is not null) throw PublishException;
        Publications.Add(new(exchange, routingKey, mandatory, basicProperties, body.ToArray()));
        Operations.Add($"publish:{routingKey}");
    }

    public override Task<QueueDeclareOk> QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete,
        IDictionary<string, object?>? arguments = null, bool passive = false, bool noWait = false,
        CancellationToken cancellationToken = default)
    {
        Require(Role.Publisher);
        cancellationToken.ThrowIfCancellationRequested();
        if (DeclareException is not null) throw DeclareException;
        durable.ShouldBeTrue();
        exclusive.ShouldBeFalse();
        autoDelete.ShouldBeFalse();
        passive.ShouldBeFalse();
        noWait.ShouldBeFalse();
        arguments.ShouldBeNull();
        DeclaredQueues.Add(queue);
        Operations.Add($"declare:{queue}");
        return Task.FromResult(new QueueDeclareOk(queue, 0, 0));
    }

    public override Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete,
        IDictionary<string, object?>? arguments = null, bool passive = false, bool noWait = false,
        CancellationToken cancellationToken = default)
    {
        Require(Role.Topology);
        cancellationToken.ThrowIfCancellationRequested();
        if (DeclareException is not null) throw DeclareException;
        passive.ShouldBeFalse();
        noWait.ShouldBeFalse();
        arguments.ShouldBeNull();
        Declarations.Add(new(exchange, type, durable, autoDelete));
        return Task.CompletedTask;
    }

    public override ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken = default)
    {
        Require(Role.Consumer);
        cancellationToken.ThrowIfCancellationRequested();
        multiple.ShouldBeFalse();
        BeforeAck?.Invoke();
        if (AckException is not null) throw AckException;
        AckedDeliveryTags.Add(deliveryTag);
        return ValueTask.CompletedTask;
    }

    public override ValueTask DisposeAsync()
    {
        Require(Role.Publisher);
        DisposeCount++;
        return ValueTask.CompletedTask;
    }
    public override void Dispose() => throw new NotSupportedException(nameof(Dispose));
    private void Require(Role role)
    {
        if (_role != role) throw new NotSupportedException($"Unexpected operation on {_role} channel; requires {role}.");
    }
}
