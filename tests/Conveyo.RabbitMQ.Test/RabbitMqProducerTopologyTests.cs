using System.Reflection;
using RabbitMQ.Client;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class RabbitMqProducerTopologyTests
{
    private sealed record ConsumedCommand(string Value);
    private sealed record ProducerOnlyEvent(string Value);

    [Test]
    public async Task DeclareProducerExchangesAsync_DeclaresMappedUrnExchanges()
    {
        var channel = TestChannel.Create();
        var options = new ConveyoContext { HostInfo = new HostInfo() };
        options._urnsByType[typeof(ProducerOnlyEvent)] = "conveyo:test.producer-only.v1";
        options._urnsByType[typeof(ConsumedCommand)] = "conveyo:test.consumed.v1";

        await RabbitMqBusRegistrationContext.DeclareProducerExchangesAsync(
            channel.Channel,
            options,
            alreadyDeclared: new HashSet<string>(StringComparer.Ordinal),
            CancellationToken.None);

        Assert.That(channel.DeclaredExchanges, Is.EquivalentTo(new[]
        {
            "conveyo:test.producer-only.v1",
            "conveyo:test.consumed.v1"
        }));
        foreach (var declaration in channel.Declarations)
        {
            Assert.That(declaration.Type, Is.EqualTo(ExchangeType.Fanout));
            Assert.That(declaration.Durable, Is.True);
            Assert.That(declaration.AutoDelete, Is.False);
        }
    }

    [Test]
    public async Task DeclareProducerExchangesAsync_SkipsFaultExchanges()
    {
        var channel = TestChannel.Create();
        var options = new ConveyoContext { HostInfo = new HostInfo() };
        options._urnsByType[typeof(ConsumedCommand)] = "conveyo:test.consumed.v1";
        options._urnsByType[typeof(Fault<ConsumedCommand>)] = "conveyo:test.consumed.v1.fault";

        await RabbitMqBusRegistrationContext.DeclareProducerExchangesAsync(
            channel.Channel,
            options,
            alreadyDeclared: new HashSet<string>(StringComparer.Ordinal),
            CancellationToken.None);

        Assert.That(channel.DeclaredExchanges, Is.EqualTo(new[] { "conveyo:test.consumed.v1" }),
            "Fault<T> exchanges should be declared lazily on first publish, not eagerly.");
    }

    [Test]
    public async Task DeclareProducerExchangesAsync_SkipsExchangesAlreadyDeclaredByConsumerLoop()
    {
        var channel = TestChannel.Create();
        var options = new ConveyoContext { HostInfo = new HostInfo() };
        options._urnsByType[typeof(ConsumedCommand)] = "conveyo:test.consumed.v1";
        options._urnsByType[typeof(ProducerOnlyEvent)] = "conveyo:test.producer-only.v1";

        var alreadyDeclared = new HashSet<string>(StringComparer.Ordinal) { "conveyo:test.consumed.v1" };

        await RabbitMqBusRegistrationContext.DeclareProducerExchangesAsync(
            channel.Channel,
            options,
            alreadyDeclared,
            CancellationToken.None);

        Assert.That(channel.DeclaredExchanges, Is.EqualTo(new[] { "conveyo:test.producer-only.v1" }));
    }

    private sealed record Declaration(string Exchange, string Type, bool Durable, bool AutoDelete);

    private class TestChannel : DispatchProxy
    {
        public IChannel Channel { get; private set; } = null!;

        public List<Declaration> Declarations { get; } = [];

        public IEnumerable<string> DeclaredExchanges => Declarations.Select(d => d.Exchange);

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

            if (targetMethod.Name == nameof(IChannel.ExchangeDeclareAsync))
            {
                Declarations.Add(new Declaration(
                    Exchange: (string)args![0]!,
                    Type: (string)args[1]!,
                    Durable: (bool)args[2]!,
                    AutoDelete: (bool)args[3]!));
                return Task.CompletedTask;
            }

            if (targetMethod.ReturnType == typeof(ValueTask))
            {
                return ValueTask.CompletedTask;
            }

            if (targetMethod.ReturnType == typeof(Task))
            {
                return Task.CompletedTask;
            }

            return null;
        }
    }
}
