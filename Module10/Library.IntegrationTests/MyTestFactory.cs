using Library.Data.PostgreSql;
using Library.Identity.Data;
using Library.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Library.IntegrationTests;

public sealed class MyTestFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");

        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<DbContextOptions<BookContext>>();
            services.RemoveAll<IDbContextOptionsConfiguration<BookContext>>();
            services.RemoveAll<BookContext>();

            services.RemoveAll<DbContextOptions<IdentityContext>>();
            services.RemoveAll<IDbContextOptionsConfiguration<IdentityContext>>();
            services.RemoveAll<IdentityContext>();

            services.AddDbContext<BookContext>(o => o.UseInMemoryDatabase("TestBookDb"));
            services.AddDbContext<IdentityContext>(o => o.UseInMemoryDatabase("TestIdentityDb"));

            var sp = services.BuildServiceProvider();
            using var scope = sp.CreateScope();
            scope.ServiceProvider.GetRequiredService<BookContext>().Database.EnsureCreated();
            scope.ServiceProvider.GetRequiredService<IdentityContext>().Database.EnsureCreated();
            
            services.AddAuthentication(options =>
                {
                    options.DefaultAuthenticateScheme = TestAuthHandler.Scheme;
                    options.DefaultChallengeScheme = TestAuthHandler.Scheme;
                })
                .AddScheme<AuthenticationSchemeOptions, TestAuthHandler>(TestAuthHandler.Scheme, _ => { });
        });
    }
}