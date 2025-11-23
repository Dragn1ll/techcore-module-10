using FluentValidation;
using Library.Contracts.Books.Request;

namespace Library.Web.Validation;

public sealed class CreateBookRequestValidator : AbstractValidator<CreateBookRequest>
{
    public CreateBookRequestValidator()
    {
        RuleFor(x => x.Title)
            .NotEmpty().WithMessage("Название книги обязательно.")
            .MaximumLength(500).WithMessage("Название книги не может превышать 500 символов.");

        RuleFor(x => x.Category)
            .IsInEnum().WithMessage("Указана недопустимая категория книги.");

        RuleFor(x => x.Authors)
            .NotEmpty().WithMessage("Автор или авторы книги обязательны.")
            .Must(authors => authors?.All(a => !string.IsNullOrWhiteSpace(a)) == true)
            .WithMessage("Все авторы должны быть непустыми строками.");

        RuleFor(x => x.Description)
            .MaximumLength(2000).WithMessage("Описание не может превышать 2000 символов.")
            .When(x => !string.IsNullOrEmpty(x.Description));

        RuleFor(x => x.Year)
            .InclusiveBetween(1900, DateTime.Now.Year).WithMessage("Год издания должен быть между 1900 и настоящем годом.");
    }
}