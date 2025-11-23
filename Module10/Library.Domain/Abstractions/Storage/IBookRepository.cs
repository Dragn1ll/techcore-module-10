using Library.Domain.Dto;
using Library.Domain.Models;

namespace Library.Domain.Abstractions.Storage;

/// <summary>
/// Репозиторий для работы с сущностями книги
/// </summary>
public interface IBookRepository
{
    /// <summary>
    /// Добавить книгу в хранилище
    /// </summary>
    /// <param name="book"></param>
    /// <returns></returns>
    Task<Guid> AddAsync(Book book);

    /// <summary>
    /// Получить книгу по идентификатору
    /// </summary>
    /// <param name="bookId">Идентификатор книги</param>
    /// <returns>Книга</returns>
    Task<Book?> GetByIdAsync(Guid bookId);

    /// <summary>
    /// Получить все книги
    /// </summary>
    /// <returns>Список книг</returns>
    Task<ICollection<Book>> GetAllAsync();

    /// <summary>
    /// Обновить данные книги
    /// </summary>
    /// <param name="book">Книга с обновлёнными данными</param>
    Task UpdateAsync(Book book);
    
    /// <summary>
    /// Удалить книгу
    /// </summary>
    /// <param name="bookId">Идентификатор книги</param>
    Task DeleteAsync(Guid bookId);

    /// <summary>
    /// Получить данные о количестве работ авторов
    /// </summary>
    /// <returns>Список количества работ авторов</returns>
    Task<ICollection<ReportDto>> GetAuthorReportAsync();
}