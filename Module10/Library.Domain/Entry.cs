using System.Net;
using System.Text;
using Library.Domain.Abstractions.Services;
using Library.Domain.Services;
using Library.SharedKernel.Enums;
using Library.SharedKernel.Utils;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Polly;
using Polly.CircuitBreaker;
using Polly.Extensions.Http;
using Polly.Timeout;

namespace Library.Domain;

public static class Entry
{
    public static IServiceCollection AddDomain(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddScoped<IBookService, BookService>();
        serviceCollection.AddSingleton<IReviewService, ReviewService>();
        serviceCollection.AddScoped<IAuthorService, AuthorService>();

        serviceCollection.AddHttpClients();

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
}
