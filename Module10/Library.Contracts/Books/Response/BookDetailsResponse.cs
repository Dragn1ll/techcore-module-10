using Library.SharedKernel.Enums;

namespace Library.Contracts.Books.Response;

/// <summary>
/// Ответ на запрос получения деталей книг
/// </summary>
/// <param name="Id">Идентификатор книги</param>
/// <param name="Title">Название книги</param>
/// <param name="Authors">Авторы книги</param>
/// <param name="Description">Описание книги</param>
/// <param name="Year">Год выпуска</param>
/// <param name="Category">Категория книги</param>
/// <param name="AverageRating">Средний рейтинг книги</param>
public sealed record BookDetailsResponse(Guid Id, string Title, IReadOnlyList<string> Authors, string Description, int Year, 
    BookCategory Category, double AverageRating);