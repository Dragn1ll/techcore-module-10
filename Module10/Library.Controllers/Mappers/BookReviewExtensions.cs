using Library.Contracts.BookReview.Request;
using Library.Contracts.BookReview.Response;
using Library.Domain.Dto;

namespace Library.Controllers.Mappers;

public static class BookReviewExtensions
{
    public static CreateBookReviewDto ToCreateBookReviewDto(this CreateBookReviewRequest request) =>
        new CreateBookReviewDto
        {
            BookId = request.BookId,
            Rating = request.Rating,
            Comment = request.Comment,
            CreatedAt = request.CreatedAt
        };

    public static GetBookReviewResponse ToGetBookReviewResponse(this GetBookReviewDto dto) =>
        new GetBookReviewResponse(dto.BookId, dto.Rating, dto.Comment, dto.CreatedAt);
}
