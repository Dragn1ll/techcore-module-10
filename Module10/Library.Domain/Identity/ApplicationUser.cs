using Microsoft.AspNetCore.Identity;

namespace Library.Domain.Identity;

public sealed class ApplicationUser : IdentityUser
{
    public DateOnly DateOfBirth { get; set; }
}
