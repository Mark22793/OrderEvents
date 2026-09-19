using System.Text;
using System.Text.Json;
using Contracts;
using RabbitMQ.Client;

const string queueName = "order.placed";

var factory = new ConnectionFactory
{
    HostName = "localhost",
    Port = 5672,
    UserName = "guest",
    Password = "guest"
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false);

var order = new OrderPlaced(
    Guid.NewGuid(),
    "student-001",
    1499.99m,
    DateTime.UtcNow);

var json = JsonSerializer.Serialize(order);
var body = Encoding.UTF8.GetBytes(json);

var properties = new BasicProperties
{
    Persistent = true,
    MessageId = order.OrderId.ToString()
};

await channel.BasicPublishAsync(
    exchange: string.Empty,
    routingKey: queueName,
    mandatory: false,
    basicProperties: properties,
    body: body);

Console.WriteLine($"Published OrderPlaced {order.OrderId}");

