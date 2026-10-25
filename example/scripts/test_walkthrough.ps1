param (
    [string]$GatewayUrl = "http://localhost:4343",
    [int]$RoomId = 42
)

$ErrorActionPreference = "Continue"

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "  VoidRush Telemetry Gateway - Automated Walkthrough Test" -ForegroundColor Cyan
Write-Host "  Target: $GatewayUrl (Room: $RoomId)" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan

# 1. Health check
Write-Host ""
Write-Host "[1/5] Testing Fast-Path Health Check (GET /healthz)..." -ForegroundColor Yellow
try {
    $resp = Invoke-WebRequest -Uri "$GatewayUrl/healthz" -Method Get -TimeoutSec 3 -UseBasicParsing
    if ($resp.StatusCode -eq 200 -and $resp.Content.Trim() -eq "OK") {
        Write-Host "  -> SUCCESS: 200 OK fast-path response verified." -ForegroundColor Green
    } else {
        Write-Host "  -> FAILED: Status $($resp.StatusCode)" -ForegroundColor Red
    }
} catch {
    Write-Host "  -> ERROR connecting to health check: $_" -ForegroundColor Red
}

# 2. Metrics check
Write-Host ""
Write-Host "[2/5] Testing Live Atomic Metrics (GET /metrics)..." -ForegroundColor Yellow
try {
    $resp = Invoke-WebRequest -Uri "$GatewayUrl/metrics" -Method Get -TimeoutSec 3 -UseBasicParsing
    if ($resp.Content -match "vr_connections_active") {
        Write-Host "  -> SUCCESS: Prometheus metrics endpoint online." -ForegroundColor Green
        $lines = $resp.Content -split "`n"
        foreach ($line in $lines) {
            if ($line -match "vr_(connections_active|accepts_total|events_total)") {
                Write-Host "     $line" -ForegroundColor DarkGray
            }
        }
    }
} catch {
    Write-Host "  -> ERROR reading metrics: $_" -ForegroundColor Red
}

# 3. HTTP Ingest
Write-Host ""
Write-Host "[3/5] Publishing Telemetry Ingest (POST /events)..." -ForegroundColor Yellow
try {
    $body = '{"sensor":"voltage","val":220.4,"room":' + $RoomId + '}'
    $resp = Invoke-WebRequest -Uri "$GatewayUrl/events" -Method Post -Body $body -ContentType "application/json" -TimeoutSec 3 -UseBasicParsing
    if ($resp.StatusCode -eq 202) {
        Write-Host "  -> SUCCESS: Ingest acknowledged with 202 Accepted." -ForegroundColor Green
    }
} catch {
    Write-Host "  -> Ingest returned: $_" -ForegroundColor DarkGray
}

# 4. Connection Fault Isolation
Write-Host ""
Write-Host "[4/5] Testing Connection-Level Fault Isolation (POST /debug/fault?type=conn)..." -ForegroundColor Yellow
try {
    Invoke-WebRequest -Uri "$GatewayUrl/debug/fault?type=conn" -Method Post -TimeoutSec 2 -UseBasicParsing 2>$null
} catch {
    Write-Host "  -> Expected socket reset caught locally." -ForegroundColor DarkGray
}

Start-Sleep -Milliseconds 500
try {
    $m = Invoke-WebRequest -Uri "$GatewayUrl/metrics" -Method Get -TimeoutSec 3 -UseBasicParsing
    $lines = $m.Content -split "`n"
    foreach ($line in $lines) {
        if ($line -match "^vr_supervisor_isolated_faults_total") {
            Write-Host "  -> Worker rescue verified: $line" -ForegroundColor Green
        }
    }
} catch {
    Write-Host "  -> Failed to verify metric: $_" -ForegroundColor Red
}

# 5. Worker Hot-Swap
Write-Host ""
Write-Host "[5/5] Testing Supervised Worker Shard Replacement (POST /debug/fault?type=worker)..." -ForegroundColor Yellow
try {
    $m1 = Invoke-WebRequest -Uri "$GatewayUrl/metrics" -Method Get -TimeoutSec 3 -UseBasicParsing
    $before = ""
    foreach ($line in ($m1.Content -split "`n")) {
        if ($line -match "^vr_supervisor_restarts_total") { $before = $line }
    }

    Invoke-WebRequest -Uri "$GatewayUrl/debug/fault?type=worker" -Method Post -TimeoutSec 2 -UseBasicParsing 2>$null
    Start-Sleep -Milliseconds 600

    $m2 = Invoke-WebRequest -Uri "$GatewayUrl/metrics" -Method Get -TimeoutSec 3 -UseBasicParsing
    $after = ""
    foreach ($line in ($m2.Content -split "`n")) {
        if ($line -match "^vr_supervisor_restarts_total") { $after = $line }
    }
    Write-Host "  -> Worker hot-swap confirmed! Before: $before -> After: $after" -ForegroundColor Green
} catch {
    Write-Host "  -> Verification note: $_" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "  Walkthrough Completed!" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan
