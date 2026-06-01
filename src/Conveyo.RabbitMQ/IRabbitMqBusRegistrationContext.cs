using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

public interface IRabbitMqBusRegistrationContext
{
    IConnection? Connection { get; }

    void RegisterConsumer<T>(string queueName) where T : class;
}
