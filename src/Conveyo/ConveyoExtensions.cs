using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Conveyo;

public static class ConveyoExtensions
{
    public static IServiceCollection AddConveyo(this IServiceCollection services, Action<IConveyoBuilder> configure)
    {
        var context = new ConveyoContext
        {
            HostInfo = GetHostInfo()
        };

        var builder = new ConveyoBuilder(services, context);
        configure(builder);

        var unmapped = context._consumerMessages.Values
            .SelectMany(messages => messages)
            .Where(i => !context._urnsByType.ContainsKey(i))
            .Distinct()
            .ToList();
        if (unmapped.Count > 0)
        {
            throw new InvalidOperationException(ErrorMessages.ConsumedMessageTypesHaveNoUrnMapping(unmapped));
        }

        services.AddSingleton<IBus, Bus>();
        services.AddHostedService(sp => ActivatorUtilities.CreateInstance<ConveyoHostedService>(sp, context));

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
