using System.Text;
using RabbitMQ.Client;
using Producer.Models;
using ClosedXML.Excel;
using System.Text.Json;

var rabbitConfig = RabbitConfiguration.GetConfig();
var factory = new ConnectionFactory
{
    HostName = rabbitConfig.Host,
    UserName = rabbitConfig.UserName,
    Password = rabbitConfig.Password
};

await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(rabbitConfig.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

var people = getPeopleWorkSheet().RowsUsed();

Console.WriteLine($"Found {people.Count() - 1} records.");

var i = 0;
foreach (var person in people)
{
    if (i == 0)
    {
        i++;
        continue;
    }

    var record = new PeopleDto(
        person.Cell(1).GetString(),
        person.Cell(2).GetString(),
        person.Cell(3).GetString(),
        person.Cell(4).GetString(),
        person.Cell(5).GetString(),
        person.Cell(6).GetString(),
        person.Cell(7).GetString(),
        person.Cell(8).GetString());

    var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(record));
    var props = new BasicProperties { Persistent = true };

    await channel.BasicPublishAsync(
    exchange: "",
    routingKey: rabbitConfig.QueueName,
    mandatory: false,
    basicProperties: props,
    body: body);

    if (i % 1000 == 0)
        Console.WriteLine($"✅ Sent {i - 1} rows...");

    i++;
}

Console.WriteLine("🎉 Done sending all records.");

static IXLWorksheet getPeopleWorkSheet()
{
    var peopleExcelPath = Path.Combine(Directory.GetCurrentDirectory(), "Files", "PeopleData.xlsx");
    var wb = new XLWorkbook(peopleExcelPath);
    var ws = wb.Worksheet(1);

    return ws;
}