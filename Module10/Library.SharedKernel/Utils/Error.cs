using Library.SharedKernel.Enums;

namespace Library.SharedKernel.Utils;

public class Error(ErrorType errorType, string errorMessage)
{
    public ErrorType ErrorType { get; } = errorType;
    public string Message { get; } = errorMessage;
}