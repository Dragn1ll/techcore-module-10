using Library.SharedKernel.Enums;

namespace Library.Domain.Dto;

/// <summary>
/// Данные для создания книги
/// </summary>
public sealed class CreateBookDto
{
    /// <summary>Название книги</summary>
    public required string Title { get; init; }

    /// <summary>Авторы</summary>
    public required IReadOnlyList<string> Authors { get; init; }

    /// <summary>Краткое описание книги</summary>
    public required string Description { get; init; }

    /// <summary>Год издания</summary>
    public required int Year { get; init; }

    /// <summary>Категория</summary>
    public required BookCategory Category { get; init; }
}