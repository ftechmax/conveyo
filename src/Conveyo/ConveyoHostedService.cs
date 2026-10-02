using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Conveyo;

internal sealed class ConveyoHostedService(
    ConveyoContext context,
    IBusRegistrationContext transport,
    MessageDispatcher dispatcher,
    ILogger<ConveyoHostedService> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(LogMessages.Starting);
        transport.OnMessageAsync += dispatcher.DispatchAsync;
        transport.OnFaultAsync += dispatcher.PublishFaultAsync;
        try
        {
            await transport.StartAsync(context, cancellationToken);
        }
        catch
        {
            Unsubscribe();
            throw;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation(LogMessages.Stopping);
        try
        {
            await transport.StopAsync(cancellationToken);
        }
        finally
        {
            Unsubscribe();
        }
    }

    private void Unsubscribe()
    {
        transport.OnMessageAsync -= dispatcher.DispatchAsync;
        transport.OnFaultAsync -= dispatcher.PublishFaultAsync;
    }
}
