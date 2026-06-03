namespace InfinityAI.Maintenance.Worker.Models;

public sealed class RabbitMqOptions
{
    public string Host        { get; set; } = "rabbitmq";
    public int    Port        { get; set; } = 5672;
    public string Username    { get; set; } = "guest";
    public string Password    { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";
}
