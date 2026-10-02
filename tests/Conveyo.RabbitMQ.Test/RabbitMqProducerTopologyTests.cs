using Microsoft.Extensions.DependencyInjection;
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
        // Arrange
        var channel = TestChannel.Topology();
        var services = new ServiceCollection();
        services.AddConveyo(builder =>
        {
            builder.Map<ProducerOnlyEvent>("conveyo:test.producer-only.v1");
            builder.Map<ConsumedCommand>("conveyo:test.consumed.v1");
        });
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<ConveyoContext>();

        // Act
        await RabbitMqBusRegistrationContext.DeclareProducerExchangesAsync(
            channel.Channel,
            options,
            alreadyDeclared: new HashSet<string>(StringComparer.Ordinal),
            CancellationToken.None);

        // Assert
        channel.DeclaredExchanges.ShouldBe(new[]
        {
            "conveyo:test.producer-only.v1",
            "conveyo:test.consumed.v1"
        }, ignoreOrder: true);
        foreach (var declaration in channel.Declarations)
        {
            declaration.Type.ShouldBe(ExchangeType.Fanout);
            declaration.Durable.ShouldBeTrue();
            declaration.AutoDelete.ShouldBeFalse();
        }
    }

    [Test]
    public async Task DeclareProducerExchangesAsync_SkipsFaultExchanges()
    {
        // Arrange
        var channel = TestChannel.Topology();
        var services = new ServiceCollection();
        services.AddConveyo(builder =>
        {
            builder.Map<ConsumedCommand>("conveyo:test.consumed.v1");
        });
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<ConveyoContext>();

        // Act
        await RabbitMqBusRegistrationContext.DeclareProducerExchangesAsync(
            channel.Channel,
            options,
            alreadyDeclared: new HashSet<string>(StringComparer.Ordinal),
            CancellationToken.None);

        // Assert
        channel.DeclaredExchanges.ShouldBe(new[] { "conveyo:test.consumed.v1" }, "Fault<T> exchanges should be declared lazily on first publish, not eagerly.");
    }

    [Test]
    public async Task DeclareProducerExchangesAsync_SkipsExchangesAlreadyDeclaredByConsumerLoop()
    {
        // Arrange
        var channel = TestChannel.Topology();
        var services = new ServiceCollection();
        services.AddConveyo(builder =>
        {
            builder.Map<ConsumedCommand>("conveyo:test.consumed.v1");
            builder.Map<ProducerOnlyEvent>("conveyo:test.producer-only.v1");
        });
        var alreadyDeclared = new HashSet<string>(StringComparer.Ordinal) { "conveyo:test.consumed.v1" };
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<ConveyoContext>();

        // Act
        await RabbitMqBusRegistrationContext.DeclareProducerExchangesAsync(
            channel.Channel,
            options,
            alreadyDeclared,
            CancellationToken.None);

        // Assert
        channel.DeclaredExchanges.ShouldBe(new[] { "conveyo:test.producer-only.v1" });
    }

}
