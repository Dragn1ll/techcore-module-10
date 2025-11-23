using Library.SharedKernel.Utils;

namespace Library.Domain.Abstractions.Services;

/// <summary>
/// Сервис для работы с авторами
/// </summary>
public interface IAuthorService
{
    /// <summary>
    /// Получить информацию из Coindesk
    /// </summary>
    /// <param name="cancellationToken">Токен для остановки выполнения метода</param>
    /// <returns>Информация из Coindesk</returns>
    Task<Result<string>> GetCoindeskInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Получить всех авторов
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns>Список авторов в виде строки</returns>
    Task<Result<string>> GetAuthorsAsync(CancellationToken cancellationToken = default);
}