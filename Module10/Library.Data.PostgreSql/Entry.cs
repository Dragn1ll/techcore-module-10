using Library.Data.PostgreSql.Repositories;
using Library.Domain.Abstractions.Storage;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Library.Data.PostgreSql;

public static class Entry
{
    public static IServiceCollection AddPostgreSql(this IServiceCollection serviceCollection, IConfiguration configuration)
    {
        serviceCollection.AddDbContext<BookContext>(options =>
        {
            options.UseNpgsql(configuration.GetConnectionString("DbConnectionString"));
        });

        serviceCollection.AddScoped<IBookRepository, BookRepository>();

        return serviceCollection;
    }
}
