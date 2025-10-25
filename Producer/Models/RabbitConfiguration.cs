namespace Producer.Models;

public record RabbitConfiguration
{
    public string Host { get; set; } = default!;
    public string UserName { get; set; } = default!;
    public string Password { get; set; } = default!;
    public string QueueName { get; set; } = default!;


    public static RabbitConfiguration GetConfig()
        => new()
        {
            Host = "localhost",
            UserName = "guest",
            Password = "guest",
            QueueName = "People"
        };
}