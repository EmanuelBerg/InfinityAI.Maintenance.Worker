using InfinityAI.Maintenance.Worker.Data;
using InfinityAI.Maintenance.Worker.Services;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information);

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException("ConnectionStrings:DefaultConnection is not configured.");

builder.Services.AddDbContext<WorkerDbContext>(options =>
    options.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)));

builder.Services.AddHttpClient<SignalRNotificationClient>();
builder.Services.AddSingleton<HeartbeatService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<HeartbeatService>());
builder.Services.AddHostedService<MaintenanceWorkerService>();

var host = builder.Build();
host.Run();
