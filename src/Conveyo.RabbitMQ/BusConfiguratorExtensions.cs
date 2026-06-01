using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Conveyo.RabbitMQ;

public static class BusConfiguratorExtensions
{
    public static void UsingRabbitMq(
        this IConveyoBuilder builder,
        Action<IRabbitMqBusRegistrationContext, RabbitMqConfiguration> configure)
    {
        var config = new RabbitMqConfiguration();
        var conveyoContext = builder.Context;
        var context = new RabbitMqBusRegistrationContext(conveyoContext);
        configure(context, config);

        var hostOptions = config.HostOptions
            ?? throw new InvalidOperationException(ErrorMessages.HostNotConfigured);
        context.SetOptions(hostOptions);

        builder.Services.AddSingleton(i =>
        {
            context.SetLogger(i.GetRequiredService<ILogger<RabbitMqBusRegistrationContext>>());
            return context;
        });
        builder.Services.AddSingleton<IRabbitMqBusRegistrationContext>(i => i.GetRequiredService<RabbitMqBusRegistrationContext>());
        builder.Services.AddSingleton<IBusRegistrationContext>(i => i.GetRequiredService<RabbitMqBusRegistrationContext>());
        builder.Services.AddSingleton<IEndpointProvider>(i => ActivatorUtilities.CreateInstance<RabbitMqEndpointProvider>(i, conveyoContext));
    }

    public static void Host(
        this RabbitMqConfiguration config,
        string host,
        string vhost,
        Action<RabbitMqHostOptions> hostOptions)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentException.ThrowIfNullOrWhiteSpace(host);
        ArgumentException.ThrowIfNullOrWhiteSpace(vhost);
        ArgumentNullException.ThrowIfNull(hostOptions);

        var assembly = Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly();
        var options = new RabbitMqHostOptions
        {
            ClientName = assembly.GetName().Name!,
            Host = host,
            Port = RabbitMqHostOptions.DefaultPort,
            VHost = vhost
        };
        hostOptions(options);

        config.HostOptions = options;
    }

    public static void Username(
        this RabbitMqHostOptions options,
        string username)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(username);

        options.Username = username;
    }

    public static void Password(
        this RabbitMqHostOptions options,
        string password)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(password);

        options.Password = password;
    }

    public static void Port(
        this RabbitMqHostOptions options,
        int port)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (port is < 1 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(port), ErrorMessages.PortOutOfRange);
        }

        options.Port = port;
    }

    public static void MaxRetries(
        this RabbitMqHostOptions options,
        int maxRetryCount)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (maxRetryCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxRetryCount), ErrorMessages.RetryCountCannotBeNegative);
        }

        options.MaxRetryCount = maxRetryCount;
    }

    public static void MaxEnvelopeSizeBytes(
        this RabbitMqHostOptions options,
        int maxBytes)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (maxBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxBytes), ErrorMessages.EnvelopeByteLimitMustBePositive);
        }

        options.MaxEnvelopeSizeBytes = maxBytes;
    }

    public static void IncludeFaultExceptionDetails(
        this RabbitMqHostOptions options,
        bool include = true)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.IncludeFaultExceptionDetails = include;
    }

    public static void PrefetchCount(
        this RabbitMqHostOptions options,
        ushort prefetchCount)
    {
        ArgumentNullException.ThrowIfNull(options);

        options.PrefetchCount = prefetchCount;
    }

    public static void UseSsl(this RabbitMqHostOptions options, Action<RabbitMqSslConfigurator>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        var configurator = new RabbitMqSslConfigurator();
        configure?.Invoke(configurator);

        options.Ssl = configurator.ToOptions(options.Host);

        if (options.Port == RabbitMqHostOptions.DefaultPort)
        {
            options.Port = RabbitMqHostOptions.TlsPort;
        }
    }
}

public class RabbitMqConfiguration
{
    public RabbitMqHostOptions? HostOptions { get; set; }

    public void ReceiveEndpoint(string queueName, Action<IEndpointConfigurator> configure)
    {
        var endpointConfigurator = new EndpointConfigurator(queueName);
        configure(endpointConfigurator);
    }
}

public interface IEndpointConfigurator
{
    void ConfigureConsumer<T>(IRabbitMqBusRegistrationContext context) where T : class;
}

internal sealed class EndpointConfigurator(string queueName) : IEndpointConfigurator
{
    public void ConfigureConsumer<T>(IRabbitMqBusRegistrationContext context) where T : class
    {
        context.RegisterConsumer<T>(queueName);
    }
}
