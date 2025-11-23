namespace Library.SharedKernel.Options;

public class MySettings
{
    public string ApplicationName { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public bool EnableSwagger { get; set; }
}