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

    throw "Không tìm thấy docker.exe. Hãy cài Docker Desktop hoặc mở PowerShell mới sau khi cài."
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
if ($LASTEXITCODE -ne 0) {
    throw "Không thể dừng môi trường Docker Compose."
}
