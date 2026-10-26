param (
    [ValidateSet("ci", "deep")]
    [string]$Profile = "ci"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RootDir = Split-Path -Parent $ScriptDir
$TlaJar = Join-Path $RootDir "plan\tla\tla2tools.jar"

if (-not (Test-Path $TlaJar)) {
    Write-Host "Downloading tla2tools.jar..." -ForegroundColor Cyan
    Invoke-WebRequest -Uri "https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar" -OutFile $TlaJar
}

if ($Profile -eq "deep") {
    Write-Host "Running deep state space exploration with VR_Reactor_Concurrency.cfg..." -ForegroundColor Yellow
    $Cfg = Join-Path $ScriptDir "tla\VR_Reactor_Concurrency.cfg"
} else {
    Write-Host "Running fast CI verification with VR_Reactor_Concurrency_CI.cfg..." -ForegroundColor Green
    $Cfg = Join-Path $ScriptDir "tla\VR_Reactor_Concurrency_CI.cfg"
}

$Tla = Join-Path $ScriptDir "tla\VR_Reactor_Concurrency.tla"

java -XX:+UseParallelGC -cp $TlaJar tlc2.TLC -deadlock -workers auto -config $Cfg $Tla
