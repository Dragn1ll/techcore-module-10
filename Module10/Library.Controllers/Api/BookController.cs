using System.Security.Claims;
using Library.Contracts.Books.Request;
using Library.Contracts.Books.Response;
using Library.Domain.Abstractions.Services;
using Library.Web.Mappers;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.OutputCaching;
using Microsoft.Extensions.Caching.Distributed;

namespace Library.Controllers.Api;

[ApiController]
[Route("api/books")]
public sealed class BookController : Controller
{
    private readonly IBookService _bookService;
    private readonly IDistributedCache _cache;
    private readonly IAnalyticsService _analyticsService;

    public BookController(IBookService bookService, IDistributedCache cache, IAnalyticsService analyticsService)
    {
        _bookService = bookService;
        _cache = cache;
        _analyticsService = analyticsService;
    }
    
    [HttpPost]
    public async Task<ActionResult> CreateBook([FromBody] CreateBookRequest request)
    {
        var result = await _bookService.CreateAsync(request.ToCreateBookDto());
        
        return result.IsSuccess 
            ? Ok(new CreateBookResponse(result.Value)) 
            : Problem(result.Error!.Message, statusCode: (int)result.Error.ErrorType);
    }
    
    [Authorize]
    [HttpGet]
    public async Task<ActionResult> GetBooks()
    {
        var result = await _bookService.GetAllAsync();
            
        return result.IsSuccess 
            ? Ok(result.Value!.Select(gbd => gbd.ToGetBookResponse()).ToList()) 
            : Problem(result.Error!.Message, statusCode: (int)result.Error.ErrorType);
    }
    
    [Authorize]
    [HttpGet("{bookId:guid}")]
    [OutputCache(PolicyName = "BookPolicy")]
    public async Task<ActionResult> GetBook([FromRoute] Guid bookId)
    {
        var result = await _bookService.GetByIdAsync(bookId);
        
        _analyticsService.PublishBookViewedAsync(bookId, User.FindFirst(ClaimTypes.NameIdentifier)?.Value 
                                                         ?? "anonymous");

        return result.IsSuccess
            ? Ok(result.Value!.ToGetBookResponse())
            : Problem(result.Error!.Message, statusCode: (int)result.Error.ErrorType);
    }
    
    [Authorize]
    [HttpPut("{id:guid}")]
    public async Task<ActionResult> UpdateBook([FromRoute] Guid id, [FromBody] UpdateBookRequest request)
    {
        var result = await _bookService.UpdateAsync(id, request.ToUpdateBookDto());
            
        return result.IsSuccess 
            ? Ok()
            : Problem(result.Error!.Message, statusCode: (int)result.Error.ErrorType);
    }

    [Authorize]
    [Authorize(Roles = "Admin")]
    [HttpDelete("{id:guid}")]
    public async Task<ActionResult> DeleteBook([FromRoute] Guid id)
    {
        var result = await _bookService.DeleteBook(id);
        
        return result.IsSuccess 
            ? Ok() 
            : Problem(result.Error!.Message, statusCode: (int)result.Error.ErrorType);
    }
    
    [Authorize]
    [HttpGet("{id}/details")]
    public async Task<ActionResult<BookDetailsResponse>> GetBookDetails(Guid id)
    {
        var book = await _bookService.GetByIdAsync(id);
        
        if (!book.IsSuccess)
        {
            return Problem(book.Error!.Message, statusCode: (int)book.Error.ErrorType);
        }
        
        var ratingString = await _cache.GetStringAsync($"rating:{id}");
        double averageRating = 0;

        if (!string.IsNullOrEmpty(ratingString) && double.TryParse(ratingString, out var parsedRating))
        {
            averageRating = parsedRating;
        }

        var details = new BookDetailsResponse(book.Value!.Id, book.Value.Title, book.Value.Authors, book.Value.Description, 
            book.Value.Year, book.Value.Category, averageRating);

        return Ok(details);
    }
    
    [Authorize]
    [Authorize(Policy = "OlderThan18")]
    [HttpGet("18+")]
    public IActionResult GetRestrictedContent()
    {
        return Ok("Этот контент могут увидеть только те, кому есть 18.");
    }
}