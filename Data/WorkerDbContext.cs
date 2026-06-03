using InfinityAI.Maintenance.Worker.Models;
using Microsoft.EntityFrameworkCore;

namespace InfinityAI.Maintenance.Worker.Data;

public sealed class WorkerDbContext(DbContextOptions<WorkerDbContext> options) : DbContext(options)
{
    public DbSet<MaintenanceJob> MaintenanceJobs => Set<MaintenanceJob>();
    public DbSet<StoredFile>     StoredFiles     => Set<StoredFile>();
    public DbSet<Document>       Documents       => Set<Document>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        base.OnModelCreating(builder);

        builder.Entity<MaintenanceJob>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.Property(x => x.JobType).HasConversion<string>().HasMaxLength(64).IsRequired();
            entity.Property(x => x.Status).HasConversion<string>().HasMaxLength(32).IsRequired().HasDefaultValue("Pending");
            entity.Property(x => x.ResultSummary).HasColumnType("longtext");
            entity.Property(x => x.ErrorMessage).HasColumnType("longtext");
            entity.ToTable("MaintenanceJobs");
        });

        builder.Entity<StoredFile>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Sha256Hash).HasMaxLength(64).IsRequired();
            entity.Property(x => x.StoragePath).HasMaxLength(1024).IsRequired();
            entity.Property(x => x.ContentType).HasMaxLength(255).IsRequired();
            entity.Property(x => x.FileExtension).HasMaxLength(32).IsRequired();
            entity.ToTable("StoredFiles");
        });

        builder.Entity<Document>(entity =>
        {
            entity.HasKey(x => x.Id);
            entity.ToTable("Documents");
        });
    }
}
