using Library.Data.PostgreSql;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using OrderWorkerService.Consumers;

var builder = Host.CreateDefaultBuilder(args);

builder.ConfigureServices((hostContext, services) =>
{
    services.AddMassTransit(x =>
    {
        var connectionString = hostContext.Configuration.GetConnectionString("DbConnectionString");
        services.AddDbContext<BookContext>(options =>
            options.UseNpgsql(connectionString));
        
        x.AddConsumer<SubmitOrderConsumer>();

        x.UsingRabbitMq((context, cfg) =>
        {
            var rabbitMqConfig = hostContext.Configuration.GetSection("MassTransit:RabbitMq");

            cfg.Host(rabbitMqConfig["Host"], "/", h =>
            {
                h.Username(rabbitMqConfig["Username"]!);
                h.Password(rabbitMqConfig["Password"]!);
            });
            
            cfg.UseMessageRetry(r => r.Incremental(5, 
                TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200)));

            cfg.ConfigureEndpoints(context);
        });
    }); 
    
});

var host = builder.Build();

await host.RunAsync();