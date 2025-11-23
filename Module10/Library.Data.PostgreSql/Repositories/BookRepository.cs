using System.Data;
using Dapper;
using Library.Data.PostgreSql.Entities;
using Library.Domain.Abstractions.Storage;
using Library.Domain.Dto;
using Library.Domain.Models;
using Microsoft.EntityFrameworkCore;

namespace Library.Data.PostgreSql.Repositories;

/// <inheritdoc cref="IBookRepository"/>
public sealed class BookRepository : IBookRepository
{
    private readonly BookContext _context;

    public BookRepository(BookContext context)
    {
        _context = context;
    }

    /// <inheritdoc cref="IBookRepository.AddAsync"/>
    public async Task<Guid> AddAsync(Book book)
    {
        await using var transaction = await _context.Database.BeginTransactionAsync();
        try
        {
            var authors = await GetAuthorsAsync(book);

            var entity = new BookEntity
            {
                Title = book.Title,
                Category = book.Category,
                Authors = authors,
                Description = book.Description,
                Year = book.Year
            };

            _context.Books.Add(entity);
            await _context.SaveChangesAsync();

            await transaction.CommitAsync();
            
            return entity.Id;
        }
        catch (Exception)
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    /// <inheritdoc cref="IBookRepository.GetByIdAsync"/>
    public async Task<Book?> GetByIdAsync(Guid bookId)
    {
        var entity = await _context.Books.AsNoTracking()
            .Include(b => b.Authors)
            .FirstOrDefaultAsync(b => b.Id == bookId);
        
        return entity == null ? null : new Book
        {
            Id = entity.Id,
            Title = entity.Title,
            Category = entity.Category,
            Authors = entity.Authors.Select(a => a.FullName).ToList(),
            Description = entity.Description,
            Year = entity.Year
        };
    }

    /// <inheritdoc cref="IBookRepository.GetAllAsync"/>
    public async Task<ICollection<Book>> GetAllAsync()
    {
        return await _context.Books
            .AsNoTracking()
            .Include(b => b.Authors)
            .Select(e => new Book
            {
                Id = e.Id,
                Title = e.Title,
                Category = e.Category,
                Authors = e.Authors.Select(a => a.FullName).ToList(),
                Description = e.Description,
                Year = e.Year
            })
            .ToListAsync();
    }

    /// <inheritdoc cref="IBookRepository.UpdateAsync"/>
    public async Task UpdateAsync(Book book)
    {
        var entity = await _context.Books.FirstAsync(b => b.Id == book.Id);
        
        entity.Title = book.Title;
        entity.Description = book.Description;
        entity.Year = book.Year;
        entity.Category = book.Category;
        entity.Authors = await GetAuthorsAsync(book);
        
        _context.Books.Update(entity);
        await _context.SaveChangesAsync();
    }

    /// <inheritdoc cref="IBookRepository.DeleteAsync"/>
    public async Task DeleteAsync(Guid bookId)
    {
        var entity = await _context.Books.FirstAsync(b => b.Id == bookId);
        
        _context.Books.Remove(entity);
        await _context.SaveChangesAsync();
    }

    /// <inheritdoc cref="IBookRepository.GetAuthorReportAsync"/>
    public async Task<ICollection<ReportDto>> GetAuthorReportAsync()
    {
        var sql = @"
                    SELECT a.""FullName"", COUNT(ba.""BookId"") as BookCount
                    FROM ""Authors"" a
                    LEFT JOIN ""BookEntityAuthorEntity"" ba ON a.""Id"" = ba.""AuthorEntityId""
                    GROUP BY a.""FullName""
                    ORDER BY BookCount DESC;
                    ";

        await using var connection = _context.Database.GetDbConnection();

        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync();
        }

        var result = await connection.QueryAsync<ReportDto>(sql);

        return result.ToList();
    }

    private async Task<ICollection<AuthorEntity>> GetAuthorsAsync(Book book)
    {
        var authors = new List<AuthorEntity>();
        foreach (var fullName in book.Authors)
        {
            var author = await _context.Authors.FirstOrDefaultAsync(a => a.FullName == fullName);
            if (author == null)
            {
                author = new AuthorEntity { FullName = fullName };
                _context.Authors.Add(author);
            }

            authors.Add(author);
        }
        
        await _context.SaveChangesAsync();
        
        return authors;
    }
}