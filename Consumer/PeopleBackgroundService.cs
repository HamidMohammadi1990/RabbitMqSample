using System.Text;
using RabbitMQ.Client;
using Consumer.Models;
using System.Text.Json;
using RabbitMQ.Client.Events;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.EntityFrameworkCore;
using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Consumer;

public class PeopleBackgroundService(IServiceProvider services, ILogger<PeopleBackgroundService> logger) : BackgroundService
{
	private readonly RabbitConfiguration configuration = RabbitConfiguration.GetConfig();

	private IChannel? channel;
	private IConnection? connection;
	private ConcurrentBag<Person> persons = [];

	public override async Task StartAsync(CancellationToken cancellationToken)
	{
		logger.LogInformation("🚀 Starting RabbitMQ Consumer...");

		var factory = new ConnectionFactory
		{
			HostName = configuration.Host,
			UserName = configuration.UserName,
			Password = configuration.Password
		};

		connection = await factory.CreateConnectionAsync(cancellationToken);
		channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

		await channel.QueueDeclareAsync(queue: configuration.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null, cancellationToken: cancellationToken);

		await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 100, global: false, cancellationToken: cancellationToken);

		logger.LogInformation("✅ Connected to RabbitMQ and queue declared: {queue}", configuration.QueueName);

		await base.StartAsync(cancellationToken);
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		if (channel is null)
		{
			logger.LogError("❌ RabbitMQ channel not initialized.");
			return;
		}

		var consumer = new AsyncEventingBasicConsumer(channel);
		consumer.ReceivedAsync += async (sender, ea) =>
		{
			var json = Encoding.UTF8.GetString(ea.Body.ToArray());

			try
			{
				var dto = JsonSerializer.Deserialize<PeopleDto>(json);
				if (dto is null)
				{
					logger.LogWarning("⚠️ Received invalid or empty message: {json}", json);
					await channel.BasicAckAsync(ea.DeliveryTag, false);
					return;
				}

				using var scope = services.CreateScope();
				var db = scope.ServiceProvider.GetRequiredService<PeopleDbContext>();
				db.ChangeTracker.AutoDetectChangesEnabled = false;

				var exists = await db.People
					.AnyAsync(p => p.ExcelId == dto.Id, stoppingToken);

				if (!exists)
				{
					var entity = new Person
					{
						ExcelId = dto.Id,
						Name = dto.Name,
						Email = dto.Email,
						Phone = dto.Phone,
						Address = dto.Address,
						Company = dto.Company,
						Description = dto.Description,
						JobTitle = dto.JobTitle
					};

					persons.Add(entity);

					if (persons.Count == 400)
					{
						db.People.AddRange(persons);
						await db.SaveChangesAsync(stoppingToken);
						persons.Clear();
					}

					logger.LogInformation("✅ Inserted record: {Email}", dto.Email);
				}
				else
				{
					logger.LogInformation("⚙️ Skipped duplicate record: {ExcelId}", dto.Id);
				}

				await channel.BasicAckAsync(ea.DeliveryTag, false);
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "❌ Error processing message");
				await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
			}
		};

		await channel.BasicConsumeAsync(queue: configuration.QueueName, autoAck: false, consumer: consumer, cancellationToken: stoppingToken);

		while (!stoppingToken.IsCancellationRequested)
			await Task.Delay(1000, stoppingToken);
	}

	public override async Task StopAsync(CancellationToken cancellationToken)
	{
		logger.LogInformation("🛑 Stopping RabbitMQ Consumer...");

		if (channel != null)
			await channel.CloseAsync(cancellationToken: cancellationToken);

		if (connection != null)
			await connection.CloseAsync(cancellationToken: cancellationToken);

		await base.StopAsync(cancellationToken);
	}

	public override void Dispose()
	{
		channel?.DisposeAsync();
		connection?.DisposeAsync();
		base.Dispose();
	}
}