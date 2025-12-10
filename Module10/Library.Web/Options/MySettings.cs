namespace Library.Web.Options;

public class MySettings
{
    public string ApplicationName { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public bool EnableSwagger { get; set; }
}