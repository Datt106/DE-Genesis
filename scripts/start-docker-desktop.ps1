$ErrorActionPreference = "Stop"

$dockerInstallPath = "C:\Program Files\Docker\Docker"
$frontendExe = Join-Path $dockerInstallPath "frontend\Docker Desktop.exe"
$backendExe = Join-Path $dockerInstallPath "resources\com.docker.backend.exe"

if (-not (Test-Path $frontendExe)) {
    throw "Khong tim thay Docker Desktop frontend tai: $frontendExe"
}

if (-not (Test-Path $backendExe)) {
    throw "Khong tim thay Docker Desktop backend tai: $backendExe"
}

Start-Process -FilePath $backendExe -WindowStyle Hidden
Start-Sleep -Seconds 5
Start-Process -FilePath $frontendExe -ArgumentList "--name=dashboard"

Write-Host "Da yeu cau Docker Desktop khoi dong. Kiem tra bang: docker info"
