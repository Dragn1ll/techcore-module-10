namespace Library.Domain.Dto;

/// <summary>
/// Данные для создания отзыва о книге
/// </summary>
public class CreateBookReviewDto
{
    /// <summary>Идентификатор книги</summary>
    public Guid BookId { get; init; }

    /// <summary>Рейтинг книги</summary>
    public int Rating { get; init; }

    /// <summary>Комментарий отзыва</summary>
    public string Comment { get; init; }

    /// <summary>Дата создания отзыва</summary>
    public DateTime CreatedAt { get; init; }
}
