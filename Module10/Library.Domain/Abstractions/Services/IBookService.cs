using Library.Domain.Dto;
using Library.SharedKernel.Utils;

namespace Library.Domain.Abstractions.Services;

/// <summary>
/// Сервис по работе с книгами
/// </summary>
public interface IBookService
{
    /// <summary>
    /// Создать книгу
    /// </summary>
    /// <param name="createBook">Данные новой книги</param>
    /// <returns>Идентификатор книги</returns>
    Task<Result<Guid>> CreateAsync(CreateBookDto createBook);
    
    /// <summary>
    /// Получить книгу по идентификатору
    /// </summary>
    /// <param name="bookId">Идентификатор книги</param>
    /// <returns></returns>
    Task<Result<GetBookDto>> GetByIdAsync(Guid bookId);
    
    /// <summary>
    /// Получить все книги
    /// </summary>
    /// <returns>Список книг</returns>
    Task<Result<ICollection<GetBookDto>>> GetAllAsync();

    /// <summary>
    /// Обновить данные книги
    /// </summary>
    /// <param name="bookId">Идентификатор книги</param>
    /// <param name="updateBook">Новые данные книги</param>
    /// <returns>Выполнилось обновление данных</returns>
    Task<Result> UpdateAsync(Guid bookId, UpdateBookDto updateBook);
    
    /// <summary>
    /// Удалить книгу по идентификатору
    /// </summary>
    /// <param name="bookId">Идентификатор книги</param>
    /// <returns>Выполнилось ли удаление</returns>
    Task<Result> DeleteBook(Guid bookId);

    /// <summary>
    /// Вызвать сервис по работе с авторами
    /// </summary>
    Task CallAuthorServiceAsync();
}