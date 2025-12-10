using Library.Domain.Abstractions.Services;
using Library.Domain.Abstractions.Storage;
using Library.Domain.Dto;
using Library.Domain.Models;
using Library.SharedKernel.Enums;
using Library.SharedKernel.Utils;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Polly.CircuitBreaker;

namespace Library.Domain.Services;

/// <inheritdoc cref="IBookService"/>
public sealed class BookService : IBookService
{
    private readonly IBookRepository _bookRepository;
    private readonly IDistributedCache _cache;
    private readonly ILogger<BookService> _logger;
    private readonly IAuthorService _authorService;

    public BookService(IBookRepository bookRepository, IDistributedCache cache, 
        ILogger<BookService> logger, IAuthorService authorService)
    {
        _bookRepository = bookRepository;
        _cache = cache;
        _logger = logger;
        _authorService = authorService;
    }

    /// <inheritdoc cref="IBookService.CreateAsync"/>
    public async Task<Result<Guid>> CreateAsync(CreateBookDto createBook)
    {
        _logger.LogInformation(
            "Создание книги с названием \"{Title}\"",
            createBook.Title);

        try
        {
            var bookId = await _bookRepository.AddAsync(new Book
            {
                Title = createBook.Title,
                Description = createBook.Description,
                Authors = createBook.Authors,
                Year = createBook.Year,
                Category = createBook.Category
            });

            _logger.LogInformation(
                "Книга {BookId} успешно создана",
                bookId);

            return Result<Guid>.Success(bookId);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ошибка при создании книги \"{Title}\"",
                createBook.Title);

            return Result<Guid>.Failure(
                new Error(ErrorType.ServerError, exception.Message));
        }
    }

    /// <inheritdoc cref="IBookService.GetByIdAsync"/>
    public async Task<Result<GetBookDto>> GetByIdAsync(Guid bookId)
    {
        var key = $"book:{bookId}";
        _logger.LogInformation(
            "Запрос книги {BookId} (ключ кэша: {CacheKey})",
            bookId, key);

        try
        {
            var cachedData = await _cache.GetAsync(key);
            Book? book = null;

            if (cachedData is { Length: > 0 })
            {
                _logger.LogDebug(
                    "Книга {BookId} найдена в кэше",
                    bookId);

                book = JsonSerializer.Deserialize<Book>(cachedData);
            }
            else
            {
                _logger.LogDebug(
                    "Книга {BookId} не найдена в кэше",
                    bookId);
            }

            if (book == null)
            {
                book = await _bookRepository.GetByIdAsync(bookId);

                if (book == null)
                {
                    _logger.LogWarning(
                        "Книга {BookId} не найдена в базе данных",
                        bookId);

                    return Result<GetBookDto>.Failure(
                        new Error(ErrorType.NotFound, "Книга не найдена."));
                }

                await _cache.SetStringAsync(key, JsonSerializer.Serialize(book));
                _logger.LogInformation(
                    "Книга {BookId} добавлена в кэш с ключом {CacheKey}",
                    bookId, key);
            }

            var dto = new GetBookDto
            {
                Id = bookId,
                Title = book.Title,
                Category = book.Category,
                Authors = book.Authors,
                Description = book.Description,
                Year = book.Year
            };

            return Result<GetBookDto>.Success(dto);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ошибка при получении книги {BookId}",
                bookId);

            return Result<GetBookDto>.Failure(
                new Error(ErrorType.ServerError, exception.Message));
        }
    }

    /// <inheritdoc cref="IBookService.GetAllAsync"/>
    public async Task<Result<ICollection<GetBookDto>>> GetAllAsync()
    {
        _logger.LogInformation("Запрос на получение списка всех книг");

        try
        {
            var books = await _bookRepository.GetAllAsync();

            var result = books
                .Select(e => new GetBookDto
                {
                    Id = e.Id,
                    Title = e.Title,
                    Category = e.Category,
                    Authors = e.Authors,
                    Description = e.Description,
                    Year = e.Year
                })
                .ToList();

            _logger.LogInformation(
                "Найдено {Count} книг(и)",
                result.Count);

            return Result<ICollection<GetBookDto>>.Success(result);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ошибка при получении списка всех книг");

            return Result<ICollection<GetBookDto>>.Failure(
                new Error(ErrorType.ServerError, exception.Message));
        }
    }

    /// <inheritdoc cref="IBookService.UpdateAsync"/>
    public async Task<Result> UpdateAsync(Guid bookId, UpdateBookDto updateBook)
    {
        _logger.LogInformation(
            "Обновление книги {BookId}",
            bookId);

        try
        {
            var book = await _bookRepository.GetByIdAsync(bookId);

            if (book == null)
            {
                _logger.LogWarning(
                    "Не удалось обновить: книга {BookId} не найдена",
                    bookId);

                return Result.Failure(
                    new Error(ErrorType.NotFound, "Книга не найдена"));
            }

            book.Title = updateBook.Title;
            book.Description = updateBook.Description;
            book.Year = updateBook.Year;

            await _bookRepository.UpdateAsync(book);
            await _cache.RemoveAsync($"book:{bookId}");

            _logger.LogInformation(
                "Книга {BookId} обновлена, запись в кэше удалена",
                bookId);

            return Result.Success();
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ошибка при обновлении книги {BookId}",
                bookId);

            return Result.Failure(
                new Error(ErrorType.ServerError, exception.Message));
        }
    }

    /// <inheritdoc cref="IBookService.DeleteBook"/>
    public async Task<Result> DeleteBook(Guid bookId)
    {
        _logger.LogInformation(
            "Удаление книги {BookId}",
            bookId);

        try
        {
            var book = await _bookRepository.GetByIdAsync(bookId);

            if (book == null)
            {
                _logger.LogWarning(
                    "Не удалось удалить: книга {BookId} не найдена",
                    bookId);

                return Result.Failure(
                    new Error(ErrorType.NotFound, "Книга не найдена"));
            }

            await _bookRepository.DeleteAsync(bookId);
            await _cache.RemoveAsync($"book:{bookId}");

            _logger.LogInformation(
                "Книга {BookId} удалена, запись в кэше удалена",
                bookId);

            return Result.Success();
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ошибка при удалении книги {BookId}",
                bookId);

            return Result.Failure(
                new Error(ErrorType.ServerError, exception.Message));
        }
    }

    /// <inheritdoc cref="IBookService.CallAuthorServiceAsync"/>
    public async Task CallAuthorServiceAsync()
    {
        try
        {
            var result = await _authorService.GetAuthorsAsync();
            Console.WriteLine($"Ответ сервиса по работе с авторами: {result}");
        }
        catch (BrokenCircuitException)
        {
            Console.WriteLine("Circuit breaker открыт; пропускаем вызов сервиса авторов");
        }
        catch (HttpRequestException ex)
        {
            Console.WriteLine($"Запрос не удался: {ex.Message}");
        }
    }
}