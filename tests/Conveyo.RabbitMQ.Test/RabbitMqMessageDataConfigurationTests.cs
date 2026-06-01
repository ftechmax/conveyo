using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace Conveyo.RabbitMQ.Test;

[TestFixture]
public class RabbitMqMessageDataConfigurationTests
{
    private sealed record ExampleMessage(string Value);

    private sealed class ExampleConsumer : IConsumer<ExampleMessage>
    {
        public Task Consume(ConsumeContext<ExampleMessage> context) => Task.CompletedTask;
    }

    [Test]
    public void RegisterConsumer_StoresQueueEndpointAddress()
    {
        var conveyoContext = new ConveyoContext { HostInfo = new HostInfo() };
        var rabbitMqContext = new RabbitMqBusRegistrationContext(conveyoContext);

        rabbitMqContext.RegisterConsumer<ExampleConsumer>("example queue");

        Assert.That(
            conveyoContext.ConsumerEndpoints[typeof(ExampleConsumer)].Single().OriginalString,
            Is.EqualTo("queue:example%20queue"));
    }

    [Test]
    public void UsingRabbitMq_ThrowsWhenHostIsNotConfigured()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddConveyo(builder =>
            {
                builder.UsingRabbitMq((_, _) => { });
            }));

        Assert.That(ex!.Message, Does.Contain("cfg.Host"));
    }

    [Test]
    public void PersistentJsonProperties_MarkMessagesAsPersistent()
    {
        var properties = RabbitMqMessageProperties.PersistentJson();

        Assert.That(properties.ContentType, Is.EqualTo("application/json"));
        Assert.That(properties.Persistent, Is.True);
        Assert.That(properties.DeliveryMode, Is.EqualTo(DeliveryModes.Persistent));
    }

    [Test]
    public void ForEnvelope_CopiesEnvelopeFieldsToBasicProperties()
    {
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();
        var sentTime = new DateTime(2026, 5, 14, 10, 30, 0, DateTimeKind.Utc);
        var envelope = new MessageEnvelope
        {
            EnvelopeVersion = MessageEnvelope.CurrentEnvelopeVersion,
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = ["conveyo:orders.order-created.v2", "conveyo:orders.order-created"],
            SentTime = sentTime
        };

        var properties = RabbitMqMessageProperties.ForEnvelope(envelope);

        Assert.That(properties.ContentType, Is.EqualTo("application/json"));
        Assert.That(properties.Persistent, Is.True);
        Assert.That(properties.MessageId, Is.EqualTo(messageId.ToString()));
        Assert.That(properties.CorrelationId, Is.EqualTo(correlationId.ToString()));
        Assert.That(properties.Type, Is.EqualTo("conveyo:orders.order-created.v2"));
        Assert.That(properties.Timestamp.UnixTime,
            Is.EqualTo(new DateTimeOffset(sentTime).ToUnixTimeSeconds()));
        Assert.That(properties.Headers, Is.Not.Null);
        Assert.That(properties.Headers!["conveyo-version"],
            Is.EqualTo(MessageEnvelope.CurrentEnvelopeVersion));
    }

    [Test]
    public void ForEnvelope_OmitsUnsetOptionalProperties()
    {
        var envelope = new MessageEnvelope
        {
            EnvelopeVersion = MessageEnvelope.CurrentEnvelopeVersion,
            MessageType = ["conveyo:test.sample.v1"]
        };

        var properties = RabbitMqMessageProperties.ForEnvelope(envelope);

        Assert.That(properties.MessageId, Is.Null.Or.Empty);
        Assert.That(properties.CorrelationId, Is.Null.Or.Empty);
        Assert.That(properties.Timestamp.UnixTime, Is.EqualTo(0));
        Assert.That(properties.Type, Is.EqualTo("conveyo:test.sample.v1"));
        Assert.That(properties.Headers!["conveyo-version"],
            Is.EqualTo(MessageEnvelope.CurrentEnvelopeVersion));
    }

    [Test]
    public void CreateConnectionFactory_UsesExternalAuthWhenCertificateIsAuthenticationIdentity()
    {
        var options = new RabbitMqHostOptions
        {
            ClientName = "test",
            Host = "localhost",
            Port = 5671,
            VHost = "/",
            Ssl = new RabbitMqSslOptions
            {
                UseCertificateAsAuthenticationIdentity = true
            }
        };

        var factory = RabbitMqConnectionManager.CreateConnectionFactory(options);
        var authMechanisms = factory.AuthMechanisms.ToList();

        Assert.That(factory.Ssl.Enabled, Is.True);
        Assert.That(authMechanisms, Has.Count.EqualTo(1));
        Assert.That(authMechanisms.Single(), Is.TypeOf<ExternalMechanismFactory>());
    }

}
