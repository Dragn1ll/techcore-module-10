using System.Text.Json;
using Confluent.Kafka;
using Library.Domain.Abstractions.Services;
using Library.Domain.Dto;

namespace Library.Domain.Services;

public class AnalyticsService : IAnalyticsService
{
    private readonly IProducer<string, string> _producer;

    public AnalyticsService(IProducer<string, string> producer)
    {
        _producer = producer;
    }

    public async Task PublishBookViewedAsync(Guid bookId, string userId, CancellationToken token = default)
    {
        var key = bookId.ToString();

        var payload = new ViewedBookDto
        {
            BookId = bookId,
            UserId = userId,
            ViewedAt = DateTime.UtcNow
        };

        var value = JsonSerializer.Serialize(payload);

        await _producer.ProduceAsync(
            "book-events",
            new Message<string, string> { Key = key, Value = value },
            token);
    }
}