namespace Library.Data.PostgreSql.Entities;

/// <summary>
/// Сущность автора
/// </summary>
public class AuthorEntity
{
    /// <summary>Идентификатор автора</summary>
    public Guid Id { get; set; }

    /// <summary>ФИО автора</summary>
    public required string FullName { get; set; }

    /// <summary>Книги автора</summary>
    public ICollection<BookEntity> Books { get; set; }
}
