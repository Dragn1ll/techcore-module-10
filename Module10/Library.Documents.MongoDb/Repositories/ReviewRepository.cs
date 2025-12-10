using Library.Documents.MongoDb.Documents;
using Library.Domain.Abstractions.Storage;
using Library.Domain.Models;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace Library.Documents.MongoDb.Repositories;

/// <inheritdoc cref="IReviewRepository"/>
public sealed class ReviewRepository : IReviewRepository
{
    private readonly IMongoCollection<BookReviewDoc> _collection;

    public ReviewRepository(IMongoClient mongoClient)
    {
        var db = mongoClient.GetDatabase("ReviewDb");
        _collection = db.GetCollection<BookReviewDoc>("bookReviews");
    }
    
    /// <inheritdoc cref="IReviewRepository.AddReviewAsync"/>
    public async Task AddReviewAsync(BookReview review)
    {
        var reviewDoc = new BookReviewDoc
        {
            BookId = review.BookId,
            Rating = review.Rating,
            Comment = review.Comment,
            CreatedAt = review.CreatedAt
        };
        
        await _collection.InsertOneAsync(reviewDoc);
    }
    
    /// <inheritdoc cref="IReviewRepository.GetReviewsForBookAsync"/>
    public async Task<List<BookReview>> GetReviewsForBookAsync(Guid bookId)
    {
        var reviews = await _collection.FindAsync(r => r.BookId == bookId);
        
        return reviews.ToList().Select(rd => new BookReview
        {
            BookId = rd.BookId,
            Rating = rd.Rating,
            Comment = rd.Comment,
            CreatedAt = rd.CreatedAt
        }).ToList();
    }

    /// <inheritdoc cref="IReviewRepository.GetAllReviewsAsync"/>
    public async Task<List<BookReview>> GetAllReviewsAsync()
    {
        return await _collection.AsQueryable()
            .Select(rd => new BookReview
            {
                BookId = rd.BookId,
                Rating = rd.Rating,
                Comment = rd.Comment,
                CreatedAt = rd.CreatedAt
            })
            .ToListAsync();
    }
}