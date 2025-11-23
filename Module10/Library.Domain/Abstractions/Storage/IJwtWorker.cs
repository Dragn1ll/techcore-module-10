using Library.Domain.Identity;

namespace Library.Domain.Abstractions.Storage;

/// <summary>
/// Генератор JWT
/// </summary>
public interface IJwtWorker
{
    /// <summary>
    /// Сгенерировать JWT по данным о пользователе
    /// </summary>
    /// <param name="user">Пользователь</param>
    /// <returns>Токен</returns>
    string GenerateToken(ApplicationUser user);
}
