namespace AnalyticsWorker.Documents;

public class BookViewDoc
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public Guid BookId { get; set; }
    public string UserId { get; set; } = null!;
    public DateTime ViewedAt { get; set; }
}