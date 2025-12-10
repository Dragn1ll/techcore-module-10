using System.Net;
using System.Text;
using Confluent.Kafka;
using Library.Domain.Abstractions.Services;
using Library.Domain.Services;
using Library.SharedKernel.Enums;
using Library.SharedKernel.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Polly;
using Polly.CircuitBreaker;
using Polly.Extensions.Http;
using Polly.Timeout;
using Error = Library.SharedKernel.Utils.Error;

namespace Library.Domain;

public static class Entry
{
    public static IServiceCollection AddDomain(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        serviceCollection.AddScoped<IBookService, BookService>();
        serviceCollection.AddSingleton<IReviewService, ReviewService>();
        serviceCollection.AddScoped<IAuthorService, AuthorService>();
        serviceCollection.AddScoped<IAnalyticsService, AnalyticsService>();

        serviceCollection.AddHttpClients();
        serviceCollection.AddKafka(configuration);

        return serviceCollection;
    }

    private static IServiceCollection AddHttpClients(this IServiceCollection serviceCollection)
    {
        var timeoutPolicy = Policy.TimeoutAsync<HttpResponseMessage>(TimeSpan.FromSeconds(3));

        var retryPolicy = HttpPolicyExtensions
            .HandleTransientHttpError()
            .WaitAndRetryAsync(3, _ => TimeSpan.FromSeconds(1));

        var circuitBreakerPolicy = HttpPolicyExtensions
            .HandleTransientHttpError()
            .CircuitBreakerAsync(5, TimeSpan.FromSeconds(30));

        var fallbackPolicy = Policy<HttpResponseMessage>
            .Handle<BrokenCircuitException>()
            .Or<TimeoutRejectedException>()
            .FallbackAsync(
                fallbackAction: _ =>
                {
                    var errorResult = Result<string>.Failure(new Error(ErrorType.ServerError, 
                        "Не удалось подключиться..."));
                    var json = JsonConvert.SerializeObject(errorResult);
                    return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
                    {
                        Content = new StringContent(json, Encoding.UTF8, "application/json")
                    });
                });
        
        var policyWrap = fallbackPolicy.WrapAsync(circuitBreakerPolicy).WrapAsync(retryPolicy).WrapAsync(timeoutPolicy);
        
        serviceCollection.AddHttpClient<IAuthorService, AuthorService>(c =>
        {
            c.BaseAddress = new Uri("https://api.coindesk.com/");
        })
        .AddPolicyHandler(policyWrap);

        return serviceCollection;
    }

    private static IServiceCollection AddKafka(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        serviceCollection.AddSingleton<IProducer<string, string>>(_ =>
        {
            var config = configuration
                             .GetSection("Kafka")
                             .Get<ProducerConfig>();
    
            return new ProducerBuilder<string, string>(config).Build();
        });
        
        return serviceCollection;
    }
}
