namespace Library.Contracts.Auth.Request;

/// <summary>
/// Запрос на регистрацию
/// </summary>
/// <param name="UserName">Имя пользователя</param>
/// <param name="Email">Электронная почта пользователя</param>
/// <param name="Password">Пароль пользователя</param>
/// <param name="BirthDay">Дата рождения пользователя</param>
public sealed record RegisterRequest(string UserName, string Email, string Password, DateOnly BirthDay);