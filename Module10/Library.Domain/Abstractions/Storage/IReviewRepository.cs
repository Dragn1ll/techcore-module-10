using Library.Domain.Models;

namespace Library.Domain.Abstractions.Storage;

/// <summary>
/// Репозиторий для работы с отзывами о книгах
/// </summary>
public interface IReviewRepository
{
    /// <summary>
    /// Добавить отзыв
    /// </summary>
    /// <param name="review">Отзыв</param>
    Task AddReviewAsync(BookReview review);

    /// <summary>
    /// Получить все отзывы о книге
    /// </summary>
    /// <param name="bookId">Идентификатор книги</param>
    /// <returns>Список отзывов</returns>
    Task<List<BookReview>> GetReviewsForBookAsync(Guid bookId);
    
    /// <summary>
    /// Получить все отзывы
    /// </summary>
    /// <returns>Список отзывов</returns>
    Task<List<BookReview>> GetAllReviewsAsync();
}