using Conveyo;
using Conveyo.RabbitMQ;
using Conveyo.Storage.Postgres;
using Weather.Contracts;
using Weather.Producer.Consumers;
using Weather.Producer.Services;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Weather.Producer;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        ConfigureServices(builder.Services, builder.Configuration);

        var app = builder.Build();

        if (app.Environment.IsDevelopment())
        {
            app.UseSwagger();
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "Weather.Producer v1");
            });
        }

        app.UseAuthorization();

        app.MapControllers();

        app.Lifetime.ApplicationStarted.Register(()
            => Console.WriteLine("[SMOKE] Weather.Producer ready"));

        app.Run();
    }

    private static void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
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

            var consumerQueue = new Uri("queue:Weather.Consumer");
            i.MapEndpointConvention<SubmitWeatherObservationCommand>(consumerQueue);
            i.MapEndpointConvention<UploadWeatherReadingsCommand>(consumerQueue);
            i.MapEndpointConvention<UploadRadarImageCommand>(consumerQueue);
            i.MapEndpointConvention<UploadSatelliteFeedCommand>(consumerQueue);
            i.MapEndpointConvention<SimulateStationFailureCommand>(consumerQueue);

            i.AddConsumer<LocalEventHandler>();

            i.UsingRabbitMq((ctx, cfg) =>
            {
                cfg.Host(configuration["rabbitmq:host"]!, "/", h =>
                {
                    h.Username(configuration["rabbitmq:username"]!);
                    h.Password(configuration["rabbitmq:password"]!);
                });

                cfg.ReceiveEndpoint("Weather.Producer", e =>
                {
                    e.ConfigureConsumer<LocalEventHandler>(ctx);
                });
            });
        });

        services.AddScoped<IWeatherService, WeatherService>();

        services.AddControllers();
        services.AddSwaggerGen();

        services.AddOpenTelemetry()
            .ConfigureResource(r => r.AddService("Weather.Producer"))
            .WithTracing(t => t
                .AddSource("Conveyo")
                .AddAspNetCoreInstrumentation()
                .AddConsoleExporter());
    }
}
