using System.Text;
using Consumer.Models;
using RabbitMQ.Client;
using System.Text.Json;
using RabbitMQ.Client.Events;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;
using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Consumer;

public class PeopleBackgroundServiceMultiMode(IServiceProvider services, ILogger<PeopleBackgroundService> logger) : BackgroundService
{
    private readonly RabbitConfiguration configuration = RabbitConfiguration.GetConfig();

    private IChannel? channel;
    private IConnection? connection;
    private readonly SemaphoreSlim saveLock = new(1, 1);
    private readonly ConcurrentQueue<Person> queue = new();

    private const int BatchSize = 500;
    private const int ConsumerCount = 300;
    private const int FlushIntervalSeconds = 5;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("🚀 Starting multi-consumer RabbitMQ background service...");

        var factory = new ConnectionFactory
        {
            HostName = configuration.Host,
            UserName = configuration.UserName,
            Password = configuration.Password
        };

        connection = await factory.CreateConnectionAsync(cancellationToken);
        channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: configuration.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);

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

        for (int i = 0; i < ConsumerCount; i++)
        {
            _ = Task.Run(() => StartConsumerAsync(i, stoppingToken), stoppingToken);
        }

        _ = Task.Run(() => AutoFlushLoop(stoppingToken), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
            await Task.Delay(1000, stoppingToken);
    }

    private async Task StartConsumerAsync(int consumerIndex, CancellationToken stoppingToken)
    {
        try
        {
            var consumer = new AsyncEventingBasicConsumer(channel!);

            consumer.ReceivedAsync += async (sender, ea) =>
            {
                var json = Encoding.UTF8.GetString(ea.Body.ToArray());

                try
                {
                    var dto = JsonSerializer.Deserialize<PeopleDto>(json);
                    if (dto is null)
                    {
                        logger.LogWarning("⚠️ [C{idx}] Invalid or empty message", consumerIndex);
                        await channel!.BasicAckAsync(ea.DeliveryTag, false);
                        return;
                    }

                    var person = new Person
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

                    queue.Enqueue(person);

                    if (queue.Count >= BatchSize)
                        await SaveBatchAsync(stoppingToken);

                    await channel!.BasicAckAsync(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "❌ [C{idx}] Error processing message", consumerIndex);
                    await channel!.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
                }
            };

            await channel!.BasicConsumeAsync(
                queue: configuration.QueueName, autoAck: false,
                consumer: consumer, cancellationToken: stoppingToken);

            logger.LogInformation("👂 Consumer {idx} started", consumerIndex);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "❌ [C{idx}] Failed to start consumer", consumerIndex);
        }
    }

    private async Task AutoFlushLoop(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(FlushIntervalSeconds), token);
                await SaveBatchAsync(token);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "💥 Error in AutoFlushLoop");
            }
        }
    }

    private async Task SaveBatchAsync(CancellationToken token)
    {
        if (queue.IsEmpty)
            return;

        await saveLock.WaitAsync(token);
        try
        {
            using var scope = services.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<PeopleDbContext>();
            db.ChangeTracker.AutoDetectChangesEnabled = false;

            var batch = new List<Person>();
            while (batch.Count < BatchSize && queue.TryDequeue(out var p))
            {
                batch.Add(p);
            }

            if (batch.Count == 0)
                return;
            
            var existingIds = await db.People
                .Where(x => batch.Select(b => b.ExcelId).Contains(x.ExcelId))
                .Select(x => x.ExcelId)
                .ToListAsync(token);

            var newItems = batch
                .Where(p => !existingIds.Contains(p.ExcelId))
                .ToList();

            if (newItems.Count > 0)
            {
                db.People.AddRange(newItems);
                await db.SaveChangesAsync(token);
                logger.LogInformation("💾 Saved batch of {count} records to database.", newItems.Count);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "❌ Error saving batch");
        }
        finally
        {
            saveLock.Release();
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("🛑 Stopping RabbitMQ Consumer...");
        await SaveBatchAsync(cancellationToken);

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