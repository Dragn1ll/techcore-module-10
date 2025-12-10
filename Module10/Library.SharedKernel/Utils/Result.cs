namespace Library.SharedKernel.Utils;

public class Result(bool isSuccess, Error? error = null)
{
    public bool IsSuccess { get; } = isSuccess;
    public Error? Error { get; } = error;
    
    public static Result Success() => new (true);
    public static Result Failure(Error? error) => new (false, error);
}

public class Result<T>(bool isSuccess, Error? error = null, T? value = default) : Result(isSuccess, error)
{
    public T? Value { get; } = value;
    
    public static Result<T> Success(T value) => new (true, null, value);
    public new static Result<T> Failure(Error? error) => new (false, error);
}