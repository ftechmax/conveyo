using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Conveyo.RabbitMQ;

internal sealed class RabbitMqConnectionManager(ILogger? logger = null)
{
    public IConnection? Connection { get; private set; }

    public IChannel? ConsumerChannel { get; private set; }

    public async Task StartAsync(RabbitMqHostOptions options, CancellationToken cancellationToken)
    {
        var factory = CreateConnectionFactory(options);
        Connection = await factory.CreateConnectionAsync(cancellationToken);
        ConsumerChannel = await Connection.CreateChannelAsync(
            options: new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                consumerDispatchConcurrency: options.ConsumerDispatchConcurrency),
            cancellationToken: cancellationToken);

        // Apply prefetch to bound the number of unacknowledged in-flight deliveries.
        await ConsumerChannel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: options.PrefetchCount,
            global: false,
            cancellationToken: cancellationToken);
    }

    public async Task<IChannel> CreatePublisherChannelAsync(CancellationToken cancellationToken)
    {
        var connection = Connection ?? throw new InvalidOperationException(ErrorMessages.ConnectionNotInitialized);
        var channel = await connection.CreateChannelAsync(
            options: new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true),
            cancellationToken: cancellationToken);

        channel.BasicReturnAsync += (_, args) =>
        {
            // Surface unroutable returns to the log. The publish path detects routing failures via
            // PublishReturnException (when publisher tracking is enabled) and wraps as UnroutableMessageException.
            logger?.LogWarning(
                LogMessages.BrokerReturnedMessage,
                args.Exchange, args.RoutingKey, args.ReplyCode, args.ReplyText);
            return Task.CompletedTask;
        };

        return channel;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (ConsumerChannel is { } consumerChannel)
        {
            await consumerChannel.CloseAsync(cancellationToken);
            consumerChannel.Dispose();
            ConsumerChannel = null;
        }

        if (Connection is { } connection)
        {
            await connection.CloseAsync(cancellationToken);
            connection.Dispose();
            Connection = null;
        }
    }

    internal static ConnectionFactory CreateConnectionFactory(RabbitMqHostOptions options)
    {
        var factory = new ConnectionFactory
        {
            HostName = options.Host,
            UserName = options.Username,
            Password = options.Password,
            VirtualHost = options.VHost,
            ClientProvidedName = options.ClientName,
            Port = options.Port,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = options.NetworkRecoveryInterval,
            RequestedHeartbeat = options.RequestedHeartbeat,
        };

        if (options.Ssl is { } ssl)
        {
            factory.Ssl = new SslOption
            {
                Enabled = true,
                ServerName = ssl.ServerName ?? options.Host,
                Version = ssl.Protocol,
                CertPath = ssl.CertificatePath ?? string.Empty,
                CertPassphrase = ssl.CertificatePassphrase ?? string.Empty,
                Certs = ssl.Certificate is null
                    ? null
                    : new System.Security.Cryptography.X509Certificates.X509CertificateCollection { ssl.Certificate },
                CertificateSelectionCallback = ssl.CertificateSelectionCallback,
                CertificateValidationCallback = ssl.CertificateValidationCallback,
                AcceptablePolicyErrors = ssl.AcceptablePolicyErrors,
                CheckCertificateRevocation = false
            };

            if (ssl.UseCertificateAsAuthenticationIdentity)
            {
                factory.AuthMechanisms = [new ExternalMechanismFactory()];
            }
        }

        return factory;
    }
}
