using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Conveyo;

public static class ConveyoExtensions
{
    public static IServiceCollection AddConveyo(this IServiceCollection services, Action<IConveyoBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        var registration = new ConveyoRegistration();
        configure(new ConveyoBuilder(services, registration));
        var context = registration.Build(GetHostInfo());

        services.AddSingleton(context);
        services.AddSingleton<IBus, Bus>();
        services.AddSingleton<MessageDataHydrator>();
        services.AddSingleton<MessageDispatcher>();
        services.AddHostedService<ConveyoHostedService>();

        return services;
    }

    private static HostInfo GetHostInfo()
    {
        var conveyoAssembly = typeof(ConveyoExtensions).Assembly;

        using var process = Process.GetCurrentProcess();

        return new HostInfo
        {
            MachineName = Environment.MachineName,
            ProcessName = process.ProcessName,
            ConveyoVersion = conveyoAssembly.GetName().Version?.ToString(),
            OperatingSystemVersion = Environment.OSVersion.VersionString,
            Runtime = "dotnet",
            RuntimeVersion = Environment.Version.ToString()
        };
    }
}
