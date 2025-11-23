using System.Text.Json;
using AnalyticsWorker.Documents;
using Confluent.Kafka;
using MongoDB.Driver;

namespace AnalyticsWorker.Consumers;

public class AnalyticsConsumer : BackgroundService
{
    private readonly ILogger<AnalyticsConsumer> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly IMongoCollection<BookViewDoc> _collection;

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
        
        var mongoClient = new MongoClient("mongodb://mongo:27017");
        var database = mongoClient.GetDatabase("analytics");
        _collection = database.GetCollection<BookViewDoc>("book_views");
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
                
                var doc = JsonSerializer.Deserialize<BookViewDoc>(result.Message.Value);

                if (doc != null)
                {
                    await _collection.InsertOneAsync(doc, cancellationToken: stoppingToken);
                }
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
