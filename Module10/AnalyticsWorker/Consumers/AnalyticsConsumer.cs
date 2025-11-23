using Confluent.Kafka;

namespace AnalyticsWorker.Consumers;

public class AnalyticsConsumer : BackgroundService
{
    private readonly ILogger<AnalyticsConsumer> _logger;
    private readonly IConsumer<string, string> _consumer;

    public AnalyticsConsumer(ILogger<AnalyticsConsumer> logger, IConfiguration configuration)
    {
        _logger = logger;

        var config = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServers"],
            GroupId = "analytics-worker",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe("book_views");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);
                
                _logger.LogInformation("Полученное сообщение: Ключ={Key}, Значение={Value}",
                    result.Message.Key, result.Message.Value);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка во время исполнения задачи из Kafka");
                await Task.Delay(1000, stoppingToken);
            }
        }
    }

    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        base.Dispose();
    }
}
