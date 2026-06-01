namespace Conveyo;

public interface IConsumer<in TMessage>
    where TMessage : class
{
    Task Consume(ConsumeContext<TMessage> context);
}
