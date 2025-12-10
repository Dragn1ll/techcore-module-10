using Library.Data.PostgreSql.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Library.Data.PostgreSql.Configurations;

public class BookConfiguration : IEntityTypeConfiguration<BookEntity>
{
    public void Configure(EntityTypeBuilder<BookEntity> builder)
    {
        builder.HasKey(b => b.Id);

        builder.Property(b => b.Title)
            .HasMaxLength(500)
            .IsRequired();

        builder.Property(b => b.Description)
            .HasMaxLength(2000)
            .IsRequired();

        builder.Property(b => b.Year)
            .IsRequired();

        builder.HasMany(b => b.Authors)
            .WithMany(a => a.Books);
    }
}
