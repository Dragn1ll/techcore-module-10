using Library.Data.PostgreSql;
using Library.Identity.Data;
using Library.Web;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Testcontainers.PostgreSql;

namespace Library.IntegrationTests;

public sealed class PostgresTestFactory : WebApplicationFactory<Program>, IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres =
        new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .Build();

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();
    }

    public new async Task DisposeAsync()
    {
        await _postgres.DisposeAsync();
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");

        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<DbContextOptions<BookContext>>();
            services.RemoveAll<BookContext>();

            services.RemoveAll<DbContextOptions<IdentityContext>>();
            services.RemoveAll<IdentityContext>();

            services.AddDbContext<BookContext>(o =>
                o.UseNpgsql(_postgres.GetConnectionString()));

            services.AddDbContext<IdentityContext>(o =>
                o.UseNpgsql(_postgres.GetConnectionString()));

            var sp = services.BuildServiceProvider();
            using var scope = sp.CreateScope();

            var appDb = scope.ServiceProvider.GetRequiredService<BookContext>();
            appDb.Database.Migrate();

            var identityDb = scope.ServiceProvider.GetRequiredService<IdentityContext>();
            identityDb.Database.Migrate();
        });
    }
}