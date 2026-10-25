$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

Write-Host "Cleaning up VoidRush Docker build artifacts..."

# Remove local build directory
$buildDir = Join-Path $ScriptDir "_build"
if (Test-Path $buildDir) {
    Write-Host "Removing $buildDir..."
    Remove-Item $buildDir -Recurse -Force
}

# Remove Linux compilation volume
Write-Host "Removing Linux EIFGENs compilation cache volume..."
docker volume rm -f voidrush_eifgens_linux 2>$null

# Stop compose if running
Write-Host "Stopping Docker Compose containers..."
Push-Location $ScriptDir
docker compose down 2>$null
Pop-Location

# Stop any orphan compilation containers
docker ps -q --filter "ancestor=voidrush/tool-eiffel-compilation" | ForEach-Object { docker rm -f $_ 2>$null }

Write-Host "Clean completed successfully."
