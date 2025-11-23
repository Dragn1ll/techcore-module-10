using Library.Domain.Abstractions.Services;
using Microsoft.Extensions.Caching.Distributed;

namespace Library.Web.BackgroundServices;

public class AverageRatingCalculatorService : BackgroundService
{
    private readonly IReviewService _reviewService;
    private readonly IDistributedCache _cache;

    public AverageRatingCalculatorService(IReviewService reviewService, IDistributedCache cache)
    {
        _reviewService = reviewService;
        _cache = cache;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await CalculateAndStoreAverageRatingsAsync();

            await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
        }
    }

    private async Task CalculateAndStoreAverageRatingsAsync()
    {
        var allReviewsResult = await _reviewService.GetAllReviewsAsync();

        if (!allReviewsResult.IsSuccess)
        {
            return;
        }

        var grouped = allReviewsResult.Value!
            .GroupBy(r => r.BookId)
            .Select(g => new
            {
                BookId = g.Key,
                AverageRating = g.Average(r => r.Rating)
            });

        foreach (var group in grouped)
        {
            var cacheKey = $"rating:{group.BookId}";
            await _cache.SetStringAsync(cacheKey, group.AverageRating.ToString("F2"));
        }
    }
}