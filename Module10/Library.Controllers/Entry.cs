using FluentValidation;
using FluentValidation.AspNetCore;
using Library.Web.Validation;
using Microsoft.Extensions.DependencyInjection;
using System.Globalization;

namespace Library.Controllers;

public static class Entry
{
    public static IServiceCollection AddRedis(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddStackExchangeRedisCache(options =>
        {
            options.Configuration = "redis:6379";
        });

        serviceCollection.AddOutputCache(options =>
        {
            options.AddPolicy("BookPolicy", policy =>
            {
                policy.Expire(TimeSpan.FromSeconds(60));
            });
        });

        return serviceCollection;
    }

    public static IMvcBuilder AddApi(this IMvcBuilder builder)
    {
        builder.Services.AddValidation();
        builder.AddApplicationPart(typeof(Api.BookController).Assembly);

        return builder;
    }

    private static void AddValidation(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddValidatorsFromAssemblyContaining<CreateBookRequestValidator>();
        serviceCollection.AddFluentValidationAutoValidation();

        ValidatorOptions.Global.DisplayNameResolver = (_, member, _) => member?.Name;
        ValidatorOptions.Global.LanguageManager.Culture = CultureInfo.GetCultureInfo("ru");
    }
}