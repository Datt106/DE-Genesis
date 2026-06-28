param(
    [switch]$RemoveVolumes
)

$ErrorActionPreference = "Stop"

function Resolve-Docker {
    $docker = Get-Command docker -ErrorAction SilentlyContinue
    if ($docker) {
        $dockerDir = Split-Path -Parent $docker.Source
        if (($env:Path -split ';') -notcontains $dockerDir) {
            $env:Path = "$dockerDir;$env:Path"
        }
        return $docker.Source
    }

    $fallback = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
    if (Test-Path $fallback) {
        $dockerDir = Split-Path -Parent $fallback
        if (($env:Path -split ';') -notcontains $dockerDir) {
            $env:Path = "$dockerDir;$env:Path"
        }
        return $fallback
    }

    throw "Khong tim thay docker.exe. Hay cai Docker Desktop hoac mo PowerShell moi sau khi cai."
}

$docker = Resolve-Docker

$composeArgs = @(
    "compose",
    "--profile", "bigdata",
    "--profile", "streaming",
    "--profile", "workflow",
    "--profile", "monitoring",
    "down"
)

if ($RemoveVolumes) {
    $composeArgs += "--volumes"
}

& $docker @composeArgs
