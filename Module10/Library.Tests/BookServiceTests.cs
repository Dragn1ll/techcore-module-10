using Library.Domain.Abstractions.Services;
using Library.Domain.Abstractions.Storage;
using Library.Domain.Models;
using Library.Domain.Services;
using Library.SharedKernel.Enums;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Moq;

namespace Library.Tests;

public class BookServiceTests
{
    private readonly Mock<IBookRepository> _repoMock = new();
    private readonly Mock<IDistributedCache> _cacheMock = new();
    private readonly Mock<IAuthorService> _authorServiceMock = new();
    private readonly BookService _service;

    public BookServiceTests(ILoggerFactory loggerFactory)
    {
        _service = new BookService(_repoMock.Object, _cacheMock.Object, loggerFactory.CreateLogger<BookService>(), 
            _authorServiceMock.Object);
    }
    
    [Fact]
    public async Task GetByIdAsync_ReturnsBook_WhenBookExists()
    {
        // Arrange
        var bookId = Guid.NewGuid();
        var book = new Book
        {
            Id = bookId,
            Title = "Test Book",
            Authors = ["Author1"],
            Description = "Description",
            Year = 2020,
            Category = BookCategory.FictionBook
        };

        _cacheMock.Setup(c => c.GetAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((byte[]?)null);

        _repoMock.Setup(r => r.GetByIdAsync(bookId))
            .ReturnsAsync(book);

        _cacheMock.Setup(c => c.SetAsync(It.IsAny<string>(), It.IsAny<byte[]>(), 
                It.IsAny<DistributedCacheEntryOptions>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        var result = await _service.GetByIdAsync(bookId);

        // Assert
        Assert.True(result.IsSuccess, $"Error: {result.Error?.Message}");
        Assert.NotNull(result.Value);
        Assert.Equal(bookId, result.Value.Id);
        Assert.Equal("Test Book", result.Value.Title);
    }
    
    
    
    [Fact]
    public async Task DeleteBook_CallsRepositoryDeleteAndCacheRemove()
    {
        // Arrange
        var bookId = Guid.NewGuid();
        var book = new Book { Id = bookId, Title = "Test Book" };

        _repoMock.Setup(r => r.GetByIdAsync(bookId))
            .ReturnsAsync(book);

        _repoMock.Setup(r => r.DeleteAsync(bookId))
            .Returns(Task.CompletedTask);

        _cacheMock.Setup(c => c.RemoveAsync(It.IsAny<string>(), CancellationToken.None))
            .Returns(Task.CompletedTask);

        // Act
        var result = await _service.DeleteBook(bookId);

        // Assert
        Assert.True(result.IsSuccess);

        _repoMock.Verify(r => r.DeleteAsync(bookId), Times.Once);
        _cacheMock.Verify(c => c.RemoveAsync($"book:{bookId}", CancellationToken.None), Times.Once);
    }
}