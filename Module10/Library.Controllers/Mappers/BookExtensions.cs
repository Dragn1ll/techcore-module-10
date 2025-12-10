using Library.Contracts.Books.Request;
using Library.Contracts.Books.Response;
using Library.Domain.Dto;

namespace Library.Web.Mappers;

public static class BookExtensions
{
    public static CreateBookDto ToCreateBookDto(this CreateBookRequest request) =>
        new()
        {
            Title = request.Title,
            Description = request.Description,
            Authors = request.Authors,
            Year = request.Year,
            Category = request.Category
        };

    public static UpdateBookDto ToUpdateBookDto(this UpdateBookRequest request) =>
        new()
        {
            Title = request.Title,
            Description = request.Description,
            Year = request.Year
        };

    public static GetBookResponse ToGetBookResponse(this GetBookDto dto) =>
        new(dto.Id, dto.Title, dto.Authors, dto.Description, dto.Year);
}