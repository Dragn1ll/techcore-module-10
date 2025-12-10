using Library.Domain.Abstractions.Services;
using Library.Domain.Abstractions.Storage;
using Library.Domain.Dto;
using Library.Domain.Models;
using Library.SharedKernel.Enums;
using Library.SharedKernel.Utils;
using Microsoft.Extensions.Logging;

namespace Library.Domain.Services;

/// <inheritdoc cref="IReviewService"/>
public class ReviewService : IReviewService
{
    private readonly IReviewRepository _reviewRepository;
    private readonly ILogger<ReviewService> _logger;

    public ReviewService(IReviewRepository reviewRepository, ILogger<ReviewService> logger)
    {
        _reviewRepository = reviewRepository;
        _logger = logger;
    }

    /// <inheritdoc cref="IReviewService.AddReviewAsync"/>
    public async Task<Result> AddReviewAsync(CreateBookReviewDto createReviewDto)
    {
        _logger.LogInformation(
            "Добавление отзыва для книги {BookId} с оценкой {Rating}",
            createReviewDto.BookId, createReviewDto.Rating);

        try
        {
            await _reviewRepository.AddReviewAsync(new BookReview
            {
                BookId = createReviewDto.BookId,
                Rating = createReviewDto.Rating,
                Comment = createReviewDto.Comment,
                CreatedAt = createReviewDto.CreatedAt,
            });

            _logger.LogInformation(
                "Отзыв для книги {BookId} успешно добавлен",
                createReviewDto.BookId);

            return Result.Success();
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ошибка при добавлении отзыва для книги {BookId}",
                createReviewDto.BookId);

            return Result.Failure(new Error(ErrorType.ServerError, exception.Message));
        }
    }

    /// <inheritdoc cref="IReviewService.GetReviewsForBookAsync"/>
    public async Task<Result<List<GetBookReviewDto>>> GetReviewsForBookAsync(Guid bookId)
    {
        _logger.LogInformation(
            "Запрос на получение отзывов для книги {BookId}",
            bookId);

        try
        {
            var reviews = (await _reviewRepository.GetReviewsForBookAsync(bookId))
                .Select(r => new GetBookReviewDto
                {
                    BookId = r.BookId,
                    Rating = r.Rating,
                    Comment = r.Comment,
                    CreatedAt = r.CreatedAt
                })
                .ToList();

            _logger.LogInformation(
                "Для книги {BookId} найдено {Count} отзыв(ов)",
                bookId, reviews.Count);

            return Result<List<GetBookReviewDto>>.Success(reviews);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ошибка при получении отзывов для книги {BookId}",
                bookId);

            return Result<List<GetBookReviewDto>>.Failure(
                new Error(ErrorType.ServerError, exception.Message));
        }
    }

    public async Task<Result<List<GetBookReviewDto>>> GetAllReviewsAsync()
    {
        _logger.LogInformation("Запрос на получение всех отзывов");

        try
        {
            var reviews = (await _reviewRepository.GetAllReviewsAsync())
                .Select(r => new GetBookReviewDto
                {
                    BookId = r.BookId,
                    Rating = r.Rating,
                    Comment = r.Comment,
                    CreatedAt = r.CreatedAt
                })
                .ToList();

            _logger.LogInformation(
                "Найдено всего {Count} отзыв(ов)",
                reviews.Count);

            return Result<List<GetBookReviewDto>>.Success(reviews);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ошибка при получении всех отзывов");

            return Result<List<GetBookReviewDto>>.Failure(
                new Error(ErrorType.ServerError, exception.Message));
        }
    }
}