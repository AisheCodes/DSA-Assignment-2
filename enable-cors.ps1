# enable-cors.ps1
# Adds a CORS annotation above every HTTP `service /x on api { ... }` declaration
# in each of the 7 services, so the browser-based demo page can call them directly.
# Safe to run more than once (skips a service if already patched).
param(
    [string]$Path = "$([Environment]::GetFolderPath('Desktop'))\food-delivery"
)

$ErrorActionPreference = "Stop"
$services = @("order-service","customer-service","restaurant-service","payment-service",
              "delivery-service","notification-service","admin-service")

$annotation = @"
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"],
        allowHeaders: ["Content-Type"]
    }
}
"@

$pattern = '(?m)^(service\s+/\S+\s+on\s+api\s*\{)'

foreach ($svc in $services) {
    $file = Join-Path $Path "$svc\main.bal"
    if (-not (Test-Path $file)) {
        Write-Host "SKIP (not found): $file" -ForegroundColor Yellow
        continue
    }
    $text = [IO.File]::ReadAllText($file)
    if ($text.Contains("cors:")) {
        Write-Host "Already patched: $svc" -ForegroundColor DarkGray
        continue
    }
    $new = [regex]::Replace($text, $pattern, { param($m) $annotation + "`n" + $m.Groups[1].Value })
    if ($new -eq $text) {
        Write-Host "WARNING: no 'service /x on api' line found in $svc/main.bal - patch it manually" -ForegroundColor Red
        continue
    }
    [IO.File]::WriteAllText($file, $new, (New-Object System.Text.UTF8Encoding($false)))
    Write-Host "Patched: $svc" -ForegroundColor Green
}

Write-Host "`nRebuilding affected containers (this can take a few minutes)..." -ForegroundColor Cyan
Set-Location $Path
docker compose up -d --build @services

Write-Host "`nDone. Open demo.html in your browser (double-click it) and try 'Set up demo data'." -ForegroundColor Green
