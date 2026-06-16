using System.Text;
using System.Text.Json;
using InfinityAI.Maintenance.Worker.Models;

namespace InfinityAI.Maintenance.Worker.Services;

public sealed class SignalRNotificationClient(
    HttpClient httpClient,
    IConfiguration configuration,
    ILogger<SignalRNotificationClient> logger)
{
    // .NET configuration maps env var SignalR__BaseUrl → SignalR:BaseUrl (__ → :)
    private string  BaseUrl     => configuration["SignalR:BaseUrl"]     ?? "http://infinityai-signalr:8080";
    private string? InternalKey => configuration["SignalR:InternalKey"];

    public async Task NotifyJobUpdatedAsync(MaintenanceJob job, CancellationToken ct = default)
    {
        var payload = new
        {
            jobId         = job.Id,
            jobType       = job.JobType.ToString(),
            status        = job.Status.ToString(),
            startedUtc    = job.StartedUtc,
            completedUtc  = job.CompletedUtc,
            resultSummary = job.ResultSummary,
            errorMessage  = job.ErrorMessage,
            createdUtc    = job.CreatedUtc
        };

        await PostInternalAsync("/internal/maintenance/job-updated", payload, ct);
    }

    private async Task PostInternalAsync(string path, object payload, CancellationToken ct)
    {
        try
        {
            var json    = JsonSerializer.Serialize(payload);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            using var request = new HttpRequestMessage(HttpMethod.Post, $"{BaseUrl}{path}");
            request.Content = content;

            if (!string.IsNullOrWhiteSpace(InternalKey))
                request.Headers.Add("X-SignalR-Internal-Key", InternalKey);

            var response = await httpClient.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
                logger.LogWarning("[SIGNALR] Notification to {Path} returned {Status}", path, response.StatusCode);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[SIGNALR] Failed to notify {Path} — real-time update skipped", path);
        }
    }
}
