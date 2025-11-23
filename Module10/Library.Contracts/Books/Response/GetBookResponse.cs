namespace Library.Contracts.Books.Response;

/// <summary>
/// Информация о книге
/// </summary>
/// <param name="Id">Идентификатор книги</param>
/// <param name="Title">Название книги</param>
/// <param name="Authors">Авторы книги</param>
/// <param name="Description">Описание книги</param>
/// <param name="Year">Год выпуска книги</param>
public sealed record GetBookResponse(
    Guid Id, 
    string Title, 
    IReadOnlyList<string> Authors, 
    string Description, 
    int Year
);