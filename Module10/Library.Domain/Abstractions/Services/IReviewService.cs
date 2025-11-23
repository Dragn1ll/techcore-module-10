using Library.Domain.Dto;
using Library.Domain.Models;
using Library.SharedKernel.Utils;

namespace Library.Domain.Abstractions.Services;

/// <summary>
/// Сервис для работы с отзывами
/// </summary>
public interface IReviewService
{
    /// <summary>
    /// Добавить отзыв
    /// </summary>
    /// <param name="review">Отзыв</param>
    Task<Result> AddReviewAsync(CreateBookReviewDto createReviewDto);

    /// <summary>
    /// Получить все отзывы о книге
    /// </summary>
    /// <param name="bookId">Идентификатор книги</param>
    /// <returns>Список отзывов</returns>
    Task<Result<List<GetBookReviewDto>>> GetReviewsForBookAsync(Guid bookId);
    
    /// <summary>
    /// Получить все отзывы
    /// </summary>
    /// <returns>Список отзывов</returns>
    Task<Result<List<GetBookReviewDto>>> GetAllReviewsAsync();
}