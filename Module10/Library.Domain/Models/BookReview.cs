namespace Library.Domain.Models;

/// <summary>
/// Отзыв
/// </summary>
public sealed class BookReview
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