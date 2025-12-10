namespace Library.Contracts.BookReview.Response;

/// <summary>
/// Ответ на запрос получения отзыва
/// </summary>
/// <param name="BookId">Идентификатор книги</param>
/// <param name="Rating">Рейтинг книги</param>
/// <param name="Comment">Комментарий отзыва</param>
/// <param name="CreatedAt">Дата создания отзыва</param>
public record GetBookReviewResponse(Guid BookId, int Rating, string Comment, DateTime CreatedAt);
