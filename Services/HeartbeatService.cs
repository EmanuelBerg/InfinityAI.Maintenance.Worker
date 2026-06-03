using InfinityAI.Maintenance.Worker.Data;
using InfinityAI.Maintenance.Worker.Models;

namespace InfinityAI.Maintenance.Worker.Services;

public sealed class HeartbeatService(
    IServiceScopeFactory scopeFactory,
    ILogger<HeartbeatService> logger) : BackgroundService
{
    private const string WorkerName = "default";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Write initial heartbeat
        await UpdateAsync("Idle", null, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            await UpdateAsync("Idle", null, stoppingToken);
        }
    }

    public async Task UpdateAsync(string status, Guid? currentJobId, CancellationToken ct)
    {
        try
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<WorkerDbContext>();

            var heartbeat = await db.WorkerHeartbeats.FindAsync([WorkerName], ct);
            if (heartbeat is null)
            {
                heartbeat = new MaintenanceWorkerHeartbeat { WorkerName = WorkerName };
                db.WorkerHeartbeats.Add(heartbeat);
            }

            heartbeat.LastSeenUtc   = DateTime.UtcNow;
            heartbeat.CurrentStatus = status;
            heartbeat.CurrentJobId  = currentJobId;

            await db.SaveChangesAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "[HEARTBEAT] Failed to update heartbeat");
        }
    }
}
