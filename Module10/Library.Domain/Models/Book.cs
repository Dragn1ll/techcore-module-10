using Library.SharedKernel.Enums;

namespace Library.Domain.Models;

/// <summary>
/// Книга
/// </summary>
public sealed class Book
{
    /// <summary>Идентификатор книги</summary>
    public Guid Id { get; init; }
    
    /// <summary>Название книги</summary>
    public string Title { get; set; }

    /// <summary>Авторы</summary>
    public IReadOnlyList<string> Authors { get; init; }

    /// <summary>Краткое описание книги</summary>
    public string Description { get; set; }

    /// <summary>Год издания</summary>
    public int Year { get; set; }

    /// <summary>Категория</summary>
    public BookCategory Category { get; init; }
}