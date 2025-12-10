namespace Library.Contracts.BookReview.Request;

/// <summary>
/// Запрос на создание отзыва о книге
/// </summary>
/// <param name="BookId">Идентификатор книги</param>
/// <param name="Rating">Рейтинг книги</param>
/// <param name="Comment">Комментарий отзыва</param>
/// <param name="CreatedAt">Дата создания отзыва</param>
public record CreateBookReviewRequest(Guid BookId, int Rating, string Comment, DateTime CreatedAt);
