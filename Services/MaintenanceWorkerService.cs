using System.Text;
using System.Text.Json;
using InfinityAI.Maintenance.Worker.Data;
using InfinityAI.Maintenance.Worker.Models;
using InfinityAI.Maintenance.Worker.Services.Handlers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace InfinityAI.Maintenance.Worker.Services;

public sealed class MaintenanceWorkerService(
    IServiceScopeFactory scopeFactory,
    IOptions<RabbitMqOptions> rabbitMqOptions,
    ILogger<MaintenanceWorkerService> logger) : BackgroundService
{
    private const string QueueName = "maintenance-jobs";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("[WORKER] Maintenance worker starting…");

        var opts = rabbitMqOptions.Value;

        logger.LogInformation(
            "[WORKER] RabbitMQ config: Host={Host} Port={Port} Username={Username} PasswordConfigured={Pwd} VirtualHost={VHost}",
            opts.Host, opts.Port, opts.Username, !string.IsNullOrWhiteSpace(opts.Password), opts.VirtualHost);

        if (string.IsNullOrWhiteSpace(opts.Password))
            throw new InvalidOperationException("RabbitMq:Password is not configured. Set the RabbitMq__Password environment variable.");

        var factory = new ConnectionFactory
        {
            HostName    = opts.Host,
            Port        = opts.Port,
            UserName    = opts.Username,
            Password    = opts.Password,
            VirtualHost = opts.VirtualHost
        };

        // Retry until RabbitMQ is reachable (Docker Swarm rolling-deploy grace)
        IConnection? connection = null;
        while (!stoppingToken.IsCancellationRequested && connection is null)
        {
            try
            {
                connection = await factory.CreateConnectionAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("[WORKER] RabbitMQ unavailable ({Msg}), retrying in 10s…", ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
        }

        if (connection is null) return;

        await using (connection)
        {
            await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

            await channel.QueueDeclareAsync(QueueName, durable: true, exclusive: false, autoDelete: false, cancellationToken: stoppingToken);
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, stoppingToken);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                MaintenanceJobMessage? msg;
                try
                {
                    msg = JsonSerializer.Deserialize<MaintenanceJobMessage>(Encoding.UTF8.GetString(ea.Body.ToArray()));
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[WORKER] Failed to deserialize message — discarding");
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                    return;
                }

                if (msg is null)
                {
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                    return;
                }

                await ProcessMessageAsync(msg, stoppingToken);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            };

            await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer, stoppingToken);
            logger.LogInformation("[WORKER] Listening on queue '{Queue}'", QueueName);

            try { await Task.Delay(Timeout.Infinite, stoppingToken); }
            catch (OperationCanceledException) { /* normal shutdown */ }
        }

        logger.LogInformation("[WORKER] Maintenance worker stopped.");
    }

    private async Task ProcessMessageAsync(MaintenanceJobMessage msg, CancellationToken ct)
    {
        // Parse the string job type from the message into the internal enum
        if (!Enum.TryParse<MaintenanceJobType>(msg.JobType, ignoreCase: true, out var jobType))
        {
            logger.LogWarning("[WORKER] Unknown JobType '{JobType}' in message {JobId} — discarding", msg.JobType, msg.JobId);
            return;
        }

        logger.LogInformation("[WORKER] Processing {JobType} (JobId={JobId})", jobType, msg.JobId);

        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<WorkerDbContext>();

        var job = await db.MaintenanceJobs.FindAsync([msg.JobId], ct);
        if (job is null)
        {
            logger.LogWarning("[WORKER] Job {JobId} not found in database — skipping", msg.JobId);
            return;
        }

        job.Status     = MaintenanceJobStatus.Running;
        job.StartedUtc = DateTime.UtcNow;
        await db.SaveChangesAsync(ct);

        try
        {
            job.ResultSummary = await DispatchAsync(jobType, db, scope.ServiceProvider, ct);
            job.Status        = MaintenanceJobStatus.Completed;
            job.CompletedUtc  = DateTime.UtcNow;
            logger.LogInformation("[WORKER] {JobType} (JobId={JobId}) completed", jobType, msg.JobId);
        }
        catch (Exception ex)
        {
            job.Status       = MaintenanceJobStatus.Failed;
            job.CompletedUtc = DateTime.UtcNow;
            job.ErrorMessage = ex.ToString();
            logger.LogError(ex, "[WORKER] {JobType} (JobId={JobId}) failed", jobType, msg.JobId);
        }

        await db.SaveChangesAsync(ct);
    }

    private async Task<string?> DispatchAsync(
        MaintenanceJobType jobType,
        WorkerDbContext db,
        IServiceProvider services,
        CancellationToken ct)
    {
        return jobType switch
        {
            MaintenanceJobType.OrphanFileScan =>
                await new OrphanFileScanHandler(db, services.GetRequiredService<ILogger<OrphanFileScanHandler>>())
                    .RunAsync(ct),

            MaintenanceJobType.OrphanDocumentScan =>
                await OrphanDocumentScanAsync(ct),

            MaintenanceJobType.OrphanQdrantScan =>
                await OrphanQdrantScanAsync(ct),

            MaintenanceJobType.SessionCleanup =>
                await SessionCleanupAsync(ct),

            _ => throw new NotSupportedException($"No handler registered for {jobType}")
        };
    }

    // ─── Placeholder handlers ─────────────────────────────────────────────────

    private async Task<string> OrphanDocumentScanAsync(CancellationToken ct)
    {
        // TODO: Scan for Documents where MergedIntoDocumentId IS NULL
        //       AND no UserDocument / ConversationDocument / KnowledgeCollectionDocument references them
        logger.LogInformation("[WORKER] OrphanDocumentScan: not yet implemented");
        await Task.CompletedTask;
        return """{"ScanType":"OrphanDocumentScan","DryRun":true,"Note":"Not yet implemented"}""";
    }

    private async Task<string> OrphanQdrantScanAsync(CancellationToken ct)
    {
        // TODO: Compare Qdrant scroll results against DocumentChunkEmbeddings in DB
        logger.LogInformation("[WORKER] OrphanQdrantScan: not yet implemented");
        await Task.CompletedTask;
        return """{"ScanType":"OrphanQdrantScan","DryRun":true,"Note":"Not yet implemented"}""";
    }

    private async Task<string> SessionCleanupAsync(CancellationToken ct)
    {
        // The Api's SessionCleanupService already handles session expiry every minute.
        // This job type is reserved for future heavy-cleanup operations (bulk end).
        logger.LogInformation("[WORKER] SessionCleanup: delegated to Api SessionCleanupService");
        await Task.CompletedTask;
        return """{"ScanType":"SessionCleanup","Note":"Handled by Api SessionCleanupService"}""";
    }
}
