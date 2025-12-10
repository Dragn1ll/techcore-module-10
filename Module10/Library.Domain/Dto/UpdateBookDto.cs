namespace Library.Domain.Dto;

/// <summary>
/// Данные для обновления книги
/// </summary>
public sealed class UpdateBookDto
{
    /// <summary>Название книги</summary>
    public required string Title { get; init; }

    /// <summary>Краткое описание книги</summary>
    public required string Description { get; init; }

    /// <summary>Год издания</summary>
    public required int Year { get; init; }
}