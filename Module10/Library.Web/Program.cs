using Library.Controllers;
using Library.Data.PostgreSql;
using Library.Documents.MongoDb;
using Library.Domain;
using Library.Identity;
using Library.Web.BackgroundServices;
using Library.Web.Extensions;
using Library.Web.Options;
using Microsoft.Extensions.Options;

namespace Library.Web;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        var services = builder.Services;

        services.AddPostgreSql(builder.Configuration);
        services.AddMongoDb(builder.Configuration);
        services.AddIdentity(builder.Configuration);
        services.AddRabbitMq(builder.Configuration);
        services.AddDomain(builder.Configuration);
        services.AddRedis();

        services.AddMvc()
            .AddApi();

        services.AddHealthChecks();

        services.AddSwagger();

        services.Configure<MySettings>(builder.Configuration.GetSection("MySettings"));

        services.AddHostedService<AverageRatingCalculatorService>();

        var app = builder.Build();

        app.AddLocalization();

        var mySettings = app.Services.GetRequiredService<IOptions<MySettings>>().Value;

        if (mySettings.EnableSwagger)
        {
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        app.MigrateDb();

        app.AddExceprionHandler();

        app.MapGet("/api/hello", () => "Hello World!");

        app.UseHttpsRedirection();

        app.UseAuthentication();
        app.UseAuthorization();

        app.UseLogMiddleware();


        app.MapControllers();
        app.MapHealthChecks("/healthz");

        app.Run();
    }
}