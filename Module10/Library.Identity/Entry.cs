using Library.Domain.Abstractions.Storage;
using Library.Domain.Identity;
using Library.Identity.Data;
using Library.Identity.Jwt;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;
using System.Text;

namespace Library.Identity;

public static class Entry
{
    public static IServiceCollection AddIdentity(this IServiceCollection serviceCollection,
        IConfiguration configuration)
    {
        serviceCollection.AddDbContext<IdentityContext>(options =>
        {
            options.UseNpgsql(configuration.GetConnectionString("DbConnectionString"));
        });

        serviceCollection.AddIdentity<ApplicationUser, IdentityRole>(options =>
            {
                options.User.RequireUniqueEmail = true;
            })
            .AddEntityFrameworkStores<IdentityContext>()
            .AddDefaultTokenProviders();

        serviceCollection.ConfigureApplicationCookie(options =>
        {
            options.Events.OnRedirectToLogin = context =>
            {
                context.Response.StatusCode = 401;
                return Task.CompletedTask;
            };
        });

        serviceCollection.Configure<JwtOptions>(configuration.GetSection(nameof(JwtOptions)));

        serviceCollection.AddScoped<IJwtWorker, JwtWorker>();

        var jwtOptions = configuration.GetSection(nameof(JwtOptions)).Get<JwtOptions>();

        serviceCollection.AddAuthentication(options =>
        {
            options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
            options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
            options.DefaultScheme = JwtBearerDefaults.AuthenticationScheme;
        })
        .AddJwtBearer(options =>
        {
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = false,
                ValidateAudience = false,
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,
                IssuerSigningKey = new SymmetricSecurityKey(
                    Encoding.UTF8.GetBytes(jwtOptions!.SecretKey))
            };
        });

        serviceCollection.AddAuthorization(options =>
        {
            options.AddPolicy("OlderThan18", policy =>
            {
                policy.RequireAuthenticatedUser();
                policy.RequireAssertion(context =>
                {
                    if (!context.User.HasClaim(c => c.Type == "DateOfBirth"))
                    {
                        return false;
                    }

                    var dobClaim = context.User.FindFirst("DateOfBirth")!.Value;
                    if (!DateTime.TryParse(dobClaim, out DateTime dob))
                    {
                        return false;
                    }

                    var age = DateTime.Today.Year - dob.Year;
                    if (dob > DateTime.Today.AddYears(-age))
                    {
                        age--;
                    }

                    return age >= 18;
                });
            });
        });

        return serviceCollection;
    }
}