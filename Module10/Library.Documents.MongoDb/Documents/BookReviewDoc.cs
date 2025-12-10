using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Library.Documents.MongoDb.Documents;

/// <summary>
/// Сущность в MongoDb для отзыва о книге
/// </summary>
public sealed class BookReviewDoc
{
    /// <summary>Идентификатор отзыва</summary>
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string Id { get; set; } = null!;
    
    /// <summary>Идентификатор книги</summary>
    public Guid BookId { get; set; }
    
    /// <summary>Рейтинг книги</summary>
    public int Rating { get; set; }
    
    /// <summary>Комментарий отзыва</summary>
    public string Comment { get; set; } = null!;
    
    /// <summary>Дата создания отзыва</summary>
    public DateTime CreatedAt { get; set; }
}