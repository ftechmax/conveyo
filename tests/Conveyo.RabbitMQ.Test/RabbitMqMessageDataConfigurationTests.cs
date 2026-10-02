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
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddConveyo(builder =>
        {
            builder.Map<ExampleMessage>("conveyo:test.example.v1");
            builder.AddConsumer<ExampleConsumer>();
            builder.UsingRabbitMq((context, rabbit) =>
            {
                rabbit.Host("localhost", "/", _ => { });
                rabbit.ReceiveEndpoint("example queue", endpoint => endpoint.ConfigureConsumer<ExampleConsumer>(context));
            });
        });
        using var provider = services.BuildServiceProvider();

        // Assert
        provider.GetRequiredService<ConveyoContext>().ConsumerEndpoints[typeof(ExampleConsumer)]
            .Single().OriginalString.ShouldBe("queue:example%20queue");
    }

    [Test]
    public void UsingRabbitMq_ThrowsWhenHostIsNotConfigured()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var ex = Should.Throw<InvalidOperationException>(() =>
            services.AddConveyo(builder =>
            {
                builder.UsingRabbitMq((_, _) => { });
            }));

        // Assert
        ex.Message.ShouldContain("cfg.Host");
    }

    [Test]
    public void PersistentJsonProperties_MarkMessagesAsPersistent()
    {
        // Arrange

        // Act
        var properties = RabbitMqMessageProperties.PersistentJson();

        // Assert
        properties.ContentType.ShouldBe("application/json");
        properties.Persistent.ShouldBeTrue();
        properties.DeliveryMode.ShouldBe(DeliveryModes.Persistent);
    }

    [Test]
    public void ForEnvelope_CopiesEnvelopeFieldsToBasicProperties()
    {
        // Arrange
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

        // Act
        var properties = RabbitMqMessageProperties.ForEnvelope(envelope);

        // Assert
        properties.ContentType.ShouldBe("application/json");
        properties.Persistent.ShouldBeTrue();
        properties.MessageId.ShouldBe(messageId.ToString());
        properties.CorrelationId.ShouldBe(correlationId.ToString());
        properties.Type.ShouldBe("conveyo:orders.order-created.v2");
        properties.Timestamp.UnixTime.ShouldBe(new DateTimeOffset(sentTime).ToUnixTimeSeconds());
        properties.Headers.ShouldNotBeNull();
        properties.Headers!["conveyo-version"].ShouldBe(MessageEnvelope.CurrentEnvelopeVersion);
    }

    [Test]
    public void ForEnvelope_OmitsUnsetOptionalProperties()
    {
        // Arrange
        var envelope = new MessageEnvelope
        {
            EnvelopeVersion = MessageEnvelope.CurrentEnvelopeVersion,
            MessageType = ["conveyo:test.sample.v1"]
        };

        // Act
        var properties = RabbitMqMessageProperties.ForEnvelope(envelope);

        // Assert
        properties.MessageId.ShouldBeNullOrEmpty();
        properties.CorrelationId.ShouldBeNullOrEmpty();
        properties.Timestamp.UnixTime.ShouldBe(0);
        properties.Type.ShouldBe("conveyo:test.sample.v1");
        properties.Headers!["conveyo-version"].ShouldBe(MessageEnvelope.CurrentEnvelopeVersion);
    }

    [Test]
    public void CreateConnectionFactory_UsesExternalAuthWhenCertificateIsAuthenticationIdentity()
    {
        // Arrange
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

        // Act
        var factory = RabbitMqConnectionManager.CreateConnectionFactory(options);
        var authMechanisms = factory.AuthMechanisms.ToList();

        // Assert
        factory.Ssl.Enabled.ShouldBeTrue();
        authMechanisms.Count().ShouldBe(1);
        authMechanisms.Single().GetType().ShouldBe(typeof(ExternalMechanismFactory));
    }

}
