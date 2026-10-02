using Microsoft.Extensions.DependencyInjection;

namespace Conveyo;

public interface IConveyoBuilder
{
    IServiceCollection Services { get; }
    internal ConveyoRegistration Registration { get; }
    void AddConsumer<T>() where T : class;
    void MapEndpointConvention<T>(Uri uri) where T : class;
    void Map<T>(string urn) where T : class;
    void MaxMessageDataBytes(long maxBytes);
    void IncludeFaultExceptionDetails(bool include = true);
}

internal sealed class ConveyoBuilder(IServiceCollection services, ConveyoRegistration registration) : IConveyoBuilder
{
    ConveyoRegistration IConveyoBuilder.Registration => registration;
    public IServiceCollection Services => services;

    public void AddConsumer<T>() where T : class
    {
        if (registration.AddConsumer(typeof(T)))
        {
            services.AddScoped<T>();
        }
    }

    public void MapEndpointConvention<T>(Uri uri) where T : class => registration.MapEndpointConvention(typeof(T), uri);
    public void Map<T>(string urn) where T : class => registration.Map(typeof(T), urn);
    public void MaxMessageDataBytes(long maxBytes) => registration.MaxMessageDataBytes(maxBytes);
    public void IncludeFaultExceptionDetails(bool include = true) => registration.IncludeFaultExceptionDetails(include);
}
