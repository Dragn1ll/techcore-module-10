using Library.Contracts.BookReview.Request;
using Library.Controllers.Mappers;
using Library.Domain.Abstractions.Services;
using Microsoft.AspNetCore.Mvc;

namespace Library.Controllers.Api;

[ApiController]
[Route("api")]
public sealed class ReviewController : Controller
{
    private readonly IReviewService _reviewService;

    public ReviewController(IReviewService reviewService)
    {
        _reviewService = reviewService;
    }
    
    [HttpPost("reviews")]
    public async Task<ActionResult> CreateReview([FromBody] CreateBookReviewRequest request)
    {
        var result = await _reviewService.AddReviewAsync(request.ToCreateBookReviewDto());
        
        return result.IsSuccess
            ? Ok()
            : Problem(result.Error!.Message, statusCode: (int)result.Error.ErrorType);
    }

    [HttpGet("books/{id:guid}/reviews")]
    public async Task<ActionResult> GetReviews([FromRoute] Guid id)
    {
        var result = await _reviewService.GetReviewsForBookAsync(id);
        
        return result.IsSuccess
            ? Ok(result.Value!.Select(r => r.ToGetBookReviewResponse()).ToList())
            : Problem(result.Error!.Message, statusCode: (int)result.Error.ErrorType);
    }
}