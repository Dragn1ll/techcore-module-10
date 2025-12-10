namespace Library.Domain.Dto;

public class ViewedBookDto
{
    public required Guid BookId { get; set; }
    public required string UserId { get; set; }
    public required DateTime ViewedAt { get; set; }
    
}