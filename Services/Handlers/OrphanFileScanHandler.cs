using System.Text.Json;
using InfinityAI.Maintenance.Worker.Data;
using Microsoft.EntityFrameworkCore;

namespace InfinityAI.Maintenance.Worker.Services.Handlers;

// Phase 4 data model chain:
//   StoredFile → Document.StoredFileId → UserDocument / ConversationDocument / KnowledgeCollectionDocument
//
// A StoredFile is orphaned when no Document has StoredFileId == sf.Id.
// The AddGlobalDocumentAssets migration guarantees that every StoredFile with any
// ApplicationFile reference also has a canonical Document, so ApplicationFile
// does not need to be checked here.

public sealed class OrphanFileScanHandler(
    WorkerDbContext db,
    ILogger<OrphanFileScanHandler> logger)
{
    public async Task<string> RunAsync(CancellationToken ct)
    {
        logger.LogInformation("[ORPHAN-FILE-SCAN] Starting dry-run scan…");

        var orphans = await db.StoredFiles
            .Where(sf => !db.Documents.Any(d => d.StoredFileId == sf.Id))
            .OrderBy(sf => sf.CreatedUtc)
            .Select(sf => new
            {
                sf.Id,
                sf.StoragePath,
                sf.Size,
                sf.ContentType,
                sf.FileExtension,
                sf.CreatedUtc
            })
            .ToListAsync(ct);

        var totalCount     = orphans.Count;
        var totalSizeBytes = orphans.Sum(o => o.Size);
        var examples = orphans.Take(20).Select(o => new
        {
            o.Id,
            o.StoragePath,
            SizeBytes  = o.Size,
            o.ContentType,
            o.FileExtension,
            CreatedUtc = o.CreatedUtc.ToString("O")
        }).ToList();

        logger.LogInformation(
            "[ORPHAN-FILE-SCAN] Found {Count} orphan StoredFiles, total {SizeMB:F1} MB",
            totalCount, totalSizeBytes / 1_048_576.0);

        return JsonSerializer.Serialize(new
        {
            ScanType       = "OrphanFileScan",
            DryRun         = true,
            OrphanCount    = totalCount,
            TotalSizeBytes = totalSizeBytes,
            TotalSizeMB    = Math.Round(totalSizeBytes / 1_048_576.0, 2),
            Examples       = examples
        });
    }
}
