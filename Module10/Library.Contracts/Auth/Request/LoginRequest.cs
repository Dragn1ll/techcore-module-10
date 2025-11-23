namespace Library.Contracts.Auth.Request;

/// <summary>
/// Запрос на логин
/// </summary>
/// <param name="Email">Электронная почта</param>
/// <param name="Password">Пароль</param>
public sealed record LoginRequest(string Email, string Password);