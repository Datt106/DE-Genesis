param(
    [ValidateSet("week1", "week2", "week3", "week4", "week5", "week6", "all")]
    [string]$Target = "week1",
    [switch]$Build
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

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Da tao .env tu .env.example"
}

$profileMap = @{
    week1 = @()
    week2 = @()
    week3 = @("bigdata")
    week4 = @("bigdata", "streaming")
    week5 = @("bigdata", "streaming", "workflow")
    week6 = @("bigdata", "streaming", "workflow", "monitoring")
    all = @("bigdata", "streaming", "workflow", "monitoring")
}

$composeArgs = @("compose")
foreach ($profile in $profileMap[$Target]) {
    $composeArgs += @("--profile", $profile)
}
$composeArgs += @("up", "-d")
if ($Build) {
    $composeArgs += "--build"
}

Write-Host "Dang khoi dong moi truong $Target..."
& $docker @composeArgs
