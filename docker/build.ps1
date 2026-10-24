<#
.SYNOPSIS
    Builds and runs the Docker container for VoidRush on Linux.

.DESCRIPTION
    Compiles and tests VoidRush (epoll native bridge and SCOOP server)
    inside a Linux container using EiffelStudio and GCC.

.PARAMETER Test
    Runs the automated test suite (default behavior).

.PARAMETER CTest
    Runs the native C epoll reactor unit tests.

.PARAMETER Valgrind
    Runs the native C epoll tests under Valgrind to verify no leaks.

.PARAMETER Server
    Runs the VoidRush server with port 8080 exposed.

.PARAMETER Shell
    Starts an interactive bash shell inside the container with sources mounted.

.PARAMETER CompileOnly
    Compiles native C and Eiffel targets without running them.

.PARAMETER Reset
    Forces a rebuild of the Docker image and cleans previous builds.

.PARAMETER NoCache
    Passes --no-cache to Docker build.
#>

param (
    [switch]$Test,
    [switch]$CTest,
    [switch]$Valgrind,
    [switch]$Server,
    [switch]$Shell,
    [switch]$CompileOnly,
    [switch]$Reset,
    [switch]$NoCache
)

$ErrorActionPreference = "Stop"

# Configuration
$ImageName = "voidrush/tool-eiffel-compilation"
$VolumeName = "voidrush_eifgens_linux"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$BUILD_DIR = Join-Path $ScriptDir "_build"
$SRC_ROOT = Resolve-Path (Join-Path $ScriptDir "..")

Write-Host "============================================================"
Write-Host "  VoidRush Linux Docker Build & Test Environment"
Write-Host "============================================================"
Write-Host "Build Directory: $BUILD_DIR"
Write-Host "Source Root:     $SRC_ROOT"

function Build-Compiler-Image {
    Write-Host "Building VoidRush compilation image: $ImageName..."
    Push-Location (Join-Path $ScriptDir "images\tools\eiffel-compilation")
    
    $opts = @()
    if ($NoCache) {
        $opts += "--no-cache"
    }
    
    docker build $opts -t $ImageName .
    if ($LASTEXITCODE -ne 0) {
        Pop-Location
        Write-Error "Failed to build compiler image."
        exit 1
    }
    Pop-Location
    Write-Host "Compiler image built successfully."
}

function Ensure-Compiler-Image {
    $exists = docker inspect $ImageName 2>$null
    if ($LASTEXITCODE -ne 0 -or $Reset) {
        Build-Compiler-Image
    } else {
        Write-Host "Compiler image $ImageName is ready."
    }
}

function Ensure-Build-Dir {
    if (-not (Test-Path $BUILD_DIR)) {
        New-Item -ItemType Directory -Path $BUILD_DIR -Force | Out-Null
    }
}

if ($Reset) {
    Write-Host "Reset requested: Cleaning build artifacts and volume..."
    if (Test-Path $BUILD_DIR) {
        Remove-Item -Recurse -Force $BUILD_DIR
    }
    docker volume rm -f $VolumeName 2>$null
}

Ensure-Compiler-Image
Ensure-Build-Dir

if ($Shell) {
    Write-Host "Launching interactive bash shell in Linux container..."
    docker run --rm -it `
        -p "8080:8080" `
        -v "${BUILD_DIR}:/opt/eiffel/build:rw" `
        -v "${SRC_ROOT}:/opt/eiffel/src:rw" `
        -v "${VolumeName}:/opt/eiffel/src/EIFGENs" `
        $ImageName /bin/bash
    exit 0
}

if ($Server) {
    Write-Host "Launching VoidRush Server in Linux container..."
    docker run --rm -it `
        -p "8080:8080" `
        -v "${BUILD_DIR}:/opt/eiffel/build:rw" `
        -v "${SRC_ROOT}:/opt/eiffel/src:rw" `
        -v "${VolumeName}:/opt/eiffel/src/EIFGENs" `
        $ImageName server
    exit 0
}

if ($CTest) {
    Write-Host "Running VoidRush native C tests in Linux container..."
    docker run --rm `
        -v "${BUILD_DIR}:/opt/eiffel/build:rw" `
        -v "${SRC_ROOT}:/opt/eiffel/src:rw" `
        -v "${VolumeName}:/opt/eiffel/src/EIFGENs" `
        $ImageName c-test
    exit $LASTEXITCODE
}

if ($Valgrind) {
    Write-Host "Running Valgrind on VoidRush native C tests in Linux container..."
    docker run --rm `
        -v "${BUILD_DIR}:/opt/eiffel/build:rw" `
        -v "${SRC_ROOT}:/opt/eiffel/src:rw" `
        -v "${VolumeName}:/opt/eiffel/src/EIFGENs" `
        $ImageName valgrind-c
    exit $LASTEXITCODE
}

if ($CompileOnly) {
    Write-Host "Compiling targets inside container..."
    docker run --rm `
        -v "${BUILD_DIR}:/opt/eiffel/build:rw" `
        -v "${SRC_ROOT}:/opt/eiffel/src:rw" `
        -v "${VolumeName}:/opt/eiffel/src/EIFGENs" `
        $ImageName compile
    exit 0
}

# Default action: run test suite
Write-Host "Running VoidRush test suite on Linux..."
docker run --rm `
    -v "${BUILD_DIR}:/opt/eiffel/build:rw" `
    -v "${SRC_ROOT}:/opt/eiffel/src:rw" `
    -v "${VolumeName}:/opt/eiffel/src/EIFGENs" `
    $ImageName test

if ($LASTEXITCODE -ne 0) {
    Write-Error "Tests failed on Linux container."
    exit 1
}

Write-Host "All Linux container tasks completed successfully."
