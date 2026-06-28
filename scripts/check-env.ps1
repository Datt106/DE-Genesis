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

Write-Host "Kiem tra Docker..."
& $docker --version
& $docker compose version

if (-not (Test-Path ".env")) {
    Write-Host "Chua co file .env. Ban co the tao bang lenh:"
    Write-Host "Copy-Item .env.example .env"
}

Write-Host ""
Write-Host "Lenh khoi dong nhanh:"
Write-Host ".\scripts\start.ps1 -Target week1 -Build"
Write-Host ""
Write-Host "Cac URL hay dung sau khi chay profile tuong ung:"
Write-Host "Spark UI:      http://localhost:8080"
Write-Host "HDFS UI:       http://localhost:9870"
Write-Host "Airflow:       http://localhost:8088"
Write-Host "NiFi:          https://localhost:8443/nifi"
Write-Host "Prometheus:    http://localhost:9090"
Write-Host "Grafana:       http://localhost:3000"
