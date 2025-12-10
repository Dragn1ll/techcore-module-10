using Library.Domain.Abstractions.Services;
using Library.SharedKernel.Enums;
using Library.SharedKernel.Utils;
using Microsoft.Extensions.Logging;

namespace Library.Domain.Services;

/// <inheritdoc cref="IAuthorService"/>
public class AuthorService : IAuthorService
{
    private readonly HttpClient _client;
    private readonly ILogger<AuthorService> _logger;

    public AuthorService(HttpClient client, ILogger<AuthorService> logger)
    {
        _client = client;
        _logger = logger;
    }

    /// <inheritdoc cref="IAuthorService.GetCoindeskInfoAsync"/>
    public async Task<Result<string>> GetCoindeskInfoAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var response = await _client.GetAsync("v1/bpi/currentprice.json", cancellationToken);
            
            response.EnsureSuccessStatusCode();
            _logger.LogInformation(
                "Получен ответ от CoinDesk: {StatusCode}", response.StatusCode);

            var result = await response.Content.ReadAsStringAsync(cancellationToken);
                
            return Result<string>.Success(result);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Ошибка при запросе к CoinDesk");
            return Result<string>.Failure(new Error(ErrorType.ServerError, exception.Message));
        }
    }

    /// <inheritdoc cref="IAuthorService.GetAuthorsAsync"/>
    public async Task<Result<string>> GetAuthorsAsync(CancellationToken cancellationToken = default)
    {
        // Симуляция подключения к серверу
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        // Искусственно выбрасываем исключение для симуляции ошибки сервера
        throw new HttpRequestException("Симуляция ошибки!");
    }
}
