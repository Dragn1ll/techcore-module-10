namespace Library.Domain.Abstractions.Services;

public interface IAnalyticsService
{
    Task PublishBookViewedAsync(Guid bookId, string userId, CancellationToken token = default);
}