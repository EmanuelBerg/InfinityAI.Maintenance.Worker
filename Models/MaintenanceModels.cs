using System.Text.Json.Serialization;

namespace InfinityAI.Maintenance.Worker.Models;

public sealed class MaintenanceJob
{
    public Guid                  Id                { get; set; }
    public MaintenanceJobType    JobType           { get; set; }
    public MaintenanceJobStatus  Status            { get; set; } = MaintenanceJobStatus.Pending;
    public Guid?                 RequestedByUserId { get; set; }
    public DateTime?             StartedUtc        { get; set; }
    public DateTime?             CompletedUtc      { get; set; }
    public string?               ResultSummary     { get; set; }
    public string?               ErrorMessage      { get; set; }
    public DateTime              CreatedUtc        { get; set; } = DateTime.UtcNow;
}

public sealed class StoredFile
{
    public Guid     Id            { get; set; }
    public string   Sha256Hash    { get; set; } = "";
    public long     Size          { get; set; }
    public string   StoragePath   { get; set; } = "";
    public string   ContentType   { get; set; } = "";
    public string   FileExtension { get; set; } = "";
    public DateTime CreatedUtc    { get; set; }
}

public sealed class Document
{
    public Guid   Id           { get; set; }
    public Guid?  StoredFileId { get; set; }
    public string Status       { get; set; } = "";
}

public sealed class MaintenanceWorkerHeartbeat
{
    public string   WorkerName    { get; set; } = "default";
    public DateTime LastSeenUtc   { get; set; }
    public string   CurrentStatus { get; set; } = "Idle";
    public Guid?    CurrentJobId  { get; set; }
}

// Message received from RabbitMQ — JobType is string for cross-service interop
public sealed class MaintenanceJobMessage
{
    public Guid    JobId       { get; set; }
    public string  JobType     { get; set; } = "";
    public string? PayloadJson { get; set; }
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum MaintenanceJobType
{
    OrphanFileScan,
    OrphanDocumentScan,
    OrphanQdrantScan,
    SessionCleanup
}

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum MaintenanceJobStatus
{
    Pending,
    Running,
    Completed,
    Failed
}
