using Library.Contracts.Books.Request;
using Library.Contracts.Books.Response;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace Library.IntegrationTests;

public class BookControllerIntegrationTests : IClassFixture<MyTestFactory>
{
    private readonly HttpClient _client;

    public BookControllerIntegrationTests(MyTestFactory factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task GetBooks_ReturnsOkAndList()
    {
        // Act
        var response = await _client.GetAsync("/api/books");

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var json = await response.Content.ReadAsStringAsync();

        var books = JsonSerializer.Deserialize<List<GetBookResponse>>(json, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        Assert.NotNull(books);
    }
    
    [Fact]
    public async Task PostBook_WithInvalidDto_ReturnsBadRequest()
    {
        // Arrange
        var badDto = new CreateBookRequest("", null!, "", -1, 0);

        // Act
        var response = await _client.PostAsJsonAsync("/api/books", badDto);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }
    
    [Fact]
    public async Task Get_WithoutToken_Returns401()
    {
        // Act
        var response = await _client.GetAsync("/api/books");

        // Assert
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }
    
    [Fact]
    public async Task Get_WithToken_Returns20Ok()
    {
        // Arrange
        _client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", "fake-test-token");
        
        // Act
        var response = await _client.GetAsync("/api/books");
        
        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}