using Library.SharedKernel.Enums;

namespace Library.Contracts.Books.Request;

/// <summary>
/// Запрос на создание книги
/// </summary>
/// <param name="Title">Название книги</param>
/// <param name="Authors">Авторы</param>
/// <param name="Description">Краткое описание книги</param>
/// <param name="Year">Год издания</param>
/// <param name="Category">Категория книги</param>
public sealed record CreateBookRequest(string Title, IReadOnlyList<string> Authors, string Description, int Year, BookCategory Category);