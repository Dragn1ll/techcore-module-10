using Library.Data.PostgreSql;
using Library.Identity.Data;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi;

namespace Library.Web.Extensions;

public static class ApiExtensions
{
    public static IServiceCollection AddSwagger(this IServiceCollection serviceCollection)
    {
        serviceCollection.AddEndpointsApiExplorer();
        serviceCollection.AddSwaggerGen(c =>
        {
            c.AddSecurityDefinition("Bearer", new OpenApiSecurityScheme
            {
                Name = "Authorization",
                In = ParameterLocation.Header,
                Type = SecuritySchemeType.Http,
                Scheme = "Bearer",
                BearerFormat = "JWT"
            });

            c.AddSecurityRequirement(odp => new OpenApiSecurityRequirement
            {
                {
                    new OpenApiSecuritySchemeReference("Bearer", odp),
                    []
                }
            });
        });

        return serviceCollection;
    }
    
    public static IServiceCollection AddRabbitMq(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        serviceCollection.AddMassTransit(x =>
        {
            x.UsingRabbitMq((context, cfg) =>
            {
                var rabbitMqConfig = configuration.GetSection("MassTransit:RabbitMq");

                cfg.Host(rabbitMqConfig["Host"], "/", h =>
                {
                    h.Username(rabbitMqConfig["Username"]!);
                    h.Password(rabbitMqConfig["Password"]!);
                });
        
                cfg.ConfigureEndpoints(context);
            });
        });

        return serviceCollection;
    }

    public static WebApplication MigrateDb(this WebApplication webApplication)
    {
        using (var scope = webApplication.Services.CreateScope())
        {
            var service = scope.ServiceProvider;
            try
            {
                var bookContext = service.GetRequiredService<BookContext>();
                bookContext.Database.Migrate();

                var identityContext = service.GetRequiredService<IdentityContext>();
                identityContext.Database.Migrate();
            }
            catch (Exception ex)
            {
                var logger = service.GetRequiredService<ILogger<Program>>();
                logger.LogError(ex, "Ошибка при применении миграций.");
            }
        }

        return webApplication;
    }

    public static IApplicationBuilder AddLocalization(this IApplicationBuilder application)
    {
        var supportedCultures = new[] { "ru-RU", "en-US" };
        var options = new RequestLocalizationOptions()
            .SetDefaultCulture("ru-RU")
            .AddSupportedCultures(supportedCultures)
            .AddSupportedUICultures(supportedCultures);
        application.UseRequestLocalization(options);

        return application;
    }

    public static IApplicationBuilder AddExceprionHandler(this IApplicationBuilder application)
    {
        var written = false;
        application.UseExceptionHandler(errorApp =>
        {
            errorApp.Run(async context =>
            {
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;

                var problemDetailsService = context.RequestServices.GetService<IProblemDetailsService>();

                if (problemDetailsService != null)
                {
                    written = await problemDetailsService.TryWriteAsync(
                        new ProblemDetailsContext { HttpContext = context });
                }

                if (!written)
                {
                    await context.Response.WriteAsync("Fallback: An error occurred.");
                }
            });
        });

        return application;
    }

    public static IApplicationBuilder UseLogMiddleware(this IApplicationBuilder application)
    {
        DateTime startTime = DateTime.UtcNow;
        application.Use(async (context, next) =>
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Запрос начал обрабатываться: {context.Request.Method} " +
                              $"{context.Request.Path}");

            await next();

            var endTime = DateTime.UtcNow;
            var executionTime = endTime - startTime;

            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Запрос обработан: {context.Request.Method} " +
                              $"{context.Request.Path} - Время выполнения: {executionTime.TotalMilliseconds} мс - " +
                              $"Статус: {context.Response.StatusCode}");
        });

        return application;
    }
}
