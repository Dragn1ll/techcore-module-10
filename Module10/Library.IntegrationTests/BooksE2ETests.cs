using Library.Contracts.Books.Request;
using Library.Contracts.Books.Response;
using Library.SharedKernel.Enums;
using Microsoft.AspNetCore.Mvc.Testing;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Library.IntegrationTests;

public sealed class BooksE2ETests : IClassFixture<PostgresTestFactory>
{
    private readonly HttpClient _client;

    public BooksE2ETests(PostgresTestFactory factory)
    {
        _client = factory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false
        });
    }
    
    [Fact]
    public async Task Post_then_Get_uses_real_Postgres_and_persists()
    {
        // POST
        // Arrange
        var request = new CreateBookRequest("E2E Title", ["Author X"], "Desc", 2024, 
            (BookCategory)1);

        // Act
        var post = await _client.PostAsJsonAsync("/api/books", request);
        
        // Assert
        Assert.Equal(HttpStatusCode.OK, post.StatusCode);

        // GET
        // Act
        var get = await _client.GetAsync("/api/books");
        get.EnsureSuccessStatusCode();

        var list = await get.Content.ReadFromJsonAsync<List<GetBookResponse>>(new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        // Assert
        Assert.NotNull(list);
        Assert.Contains(list, b => b.Title == "E2E Title");
    }
}