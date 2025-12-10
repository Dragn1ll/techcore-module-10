namespace Library.Domain.Dto;

/// <summary>
/// Данные о количестве работ автора
/// </summary>
public sealed class ReportDto
{
    /// <summary>
    /// ФИО автора
    /// </summary>
    public required string FullName { get; set; }
    
    /// <summary>
    /// Количество написанных книг
    /// </summary>
    public required int BookCount { get; set; }
}