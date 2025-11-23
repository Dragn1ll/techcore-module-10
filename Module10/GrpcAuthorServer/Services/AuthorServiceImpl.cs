using Grpc.Core;

namespace GrpcAuthorServer.Services;

public class AuthorServiceImpl : AuthorService.AuthorServiceBase
{
    private readonly ILogger<AuthorServiceImpl> _logger;
    public AuthorServiceImpl(ILogger<AuthorServiceImpl> logger)
    {
        _logger = logger;
    }

    public override Task<AuthorReply> GetAuthor(GetAuthorRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Запрос на автора с ID: {id}", request.AuthorId);

        // Временная заглушка
        return Task.FromResult(new AuthorReply
        {
            AuthorId = request.AuthorId,
            Name = "Лев Толстой",
            Biography = "Великий русский писатель..."
        });
    }
}