using FluentValidation;
using Library.Contracts.Books.Request;

namespace Library.Web.Validation;

public class UpdateBookRequestValidator : AbstractValidator<UpdateBookRequest>
{
    public UpdateBookRequestValidator()
    {
        RuleFor(x => x.Title)
            .NotEmpty().WithMessage("Название книги обязательно.")
            .MaximumLength(500).WithMessage("Название книги не может превышать 500 символов.");
        
        RuleFor(x => x.Description)
            .MaximumLength(2000).WithMessage("Описание не может превышать 2000 символов.")
            .When(x => !string.IsNullOrEmpty(x.Description));

        RuleFor(x => x.Year)
            .InclusiveBetween(1800, DateTime.Now.Year).WithMessage("Год издания должен быть между 1900 и настоящем годом.");
    }
}