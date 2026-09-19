using System.Text;
using System.Text.Json;
using Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = "localhost" };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

const string queueName = "order.placed";

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

// Quality of Service: Prefetch 10 messages max
await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false);

Console.WriteLine(" [*] Waiting for orders. To exit press CTRL+C");

var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);

    try
    {
        var order = JsonSerializer.Deserialize<OrderPlaced>(message);
        Console.WriteLine($" [x] Processed Order {order?.OrderId} for Student {order?.StudentId} (Amount: ${order?.Total})");

        // Simulate work
        await Task.Delay(500);

        // Explicit acknowledgment
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (Exception ex)
    {
        Console.WriteLine($" [!] Error processing message: {ex.Message}");

        // Requeue: false pushes the message to DLQ (if configured) or drops it safely
        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
    }
};

await channel.BasicConsumeAsync(
    queue: queueName,
    autoAck: false, // Manual acknowledgment enabled
    consumer: consumer);

// Keep app running until stopped
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, eventArgs) =>
{
    eventArgs.Cancel = true;
    cts.Cancel();
};

await Task.Delay(Timeout.Infinite, cts.Token).ContinueWith(_ => { });
