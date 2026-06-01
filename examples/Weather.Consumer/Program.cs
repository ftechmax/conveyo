using Conveyo;
using Conveyo.RabbitMQ;
using Conveyo.Storage.Postgres;
using Weather.Consumer.Consumers;
using Weather.Contracts;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Weather.Consumer;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = Host.CreateDefaultBuilder(args);

        builder.ConfigureServices(ConfigureServices);

        var app = builder.Build();

        app.Services
            .GetRequiredService<IHostApplicationLifetime>()
            .ApplicationStarted.Register(()
                => Console.WriteLine("[SMOKE] Weather.Consumer ready"));

        await app.RunAsync();
    }

    private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
    {
        var configuration = context.Configuration;
        var backend = (configuration["conveyo:messageData:backend"] ?? "postgres").ToLowerInvariant();

        services.AddConveyo(i =>
        {
            switch (backend)
            {
                case "postgres":
                    i.AddPostgresMessageData(
                        configuration["postgres:connection-string"]!);
                    break;
                case "none":
                    break;
                default:
                    throw new InvalidOperationException(
                        $"Unknown conveyo:messageData:backend value '{backend}'. Use 'postgres' or 'none'.");
            }

            i.MapWeatherContracts();

            i.AddConsumer<SubmitWeatherObservationConsumer>();
            i.AddConsumer<WeatherSampleConsumer>();
            i.AddConsumer<SimulateStationFailureConsumer>();

            i.UsingRabbitMq((ctx, cfg) =>
            {
                cfg.Host(configuration["rabbitmq:host"]!, "/", h =>
                {
                    h.Username(configuration["rabbitmq:username"]!);
                    h.Password(configuration["rabbitmq:password"]!);
                });

                cfg.ReceiveEndpoint("Weather.Consumer", e =>
                {
                    e.ConfigureConsumer<SubmitWeatherObservationConsumer>(ctx);
                    e.ConfigureConsumer<WeatherSampleConsumer>(ctx);
                    e.ConfigureConsumer<SimulateStationFailureConsumer>(ctx);
                });
            });
        });
        services.AddScoped<IApplicationService, ApplicationService>();

        services.AddOpenTelemetry()
            .ConfigureResource(r => r.AddService("Weather.Consumer"))
            .WithTracing(t => t
                .AddSource("Conveyo")
                .AddConsoleExporter());
    }
}
