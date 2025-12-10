using AnalyticsWorker.Consumers;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddHostedService<AnalyticsConsumer>();
    })
    .Build();

await host.RunAsync();
