namespace Library.Tests;

public class SimpleTests
{
    [Fact]
    public void Pow_ReturnsCorrectSquareNumber()
    {
        var result = Math.Pow(2, 2);
        
        Assert.Equal(4, result);
    }
    
    [Theory]
    [InlineData(1, 2, 1)]
    [InlineData(5, 5, 3125)]
    public void Pow_ReturnsExpectedResult(double x, double y, double expected)
    {
        var result = Math.Pow(x, y);

        Assert.Equal(expected, result, precision: 10);
    }
}