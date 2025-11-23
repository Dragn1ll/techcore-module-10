using Library.Data.PostgreSql.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Library.Data.PostgreSql.Configurations;

public class AuthorConfiguration : IEntityTypeConfiguration<AuthorEntity>
{
    public void Configure(EntityTypeBuilder<AuthorEntity> builder)
    {
        builder.HasKey(a => a.Id);

        builder.Property(a => a.FullName).IsRequired();

        builder.HasIndex(a => a.FullName).IsUnique();

        builder.HasMany(a => a.Books)
            .WithMany(b => b.Authors);
    }
}
