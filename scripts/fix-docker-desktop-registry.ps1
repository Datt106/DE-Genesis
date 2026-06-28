param(
    [string]$DockerInstallPath = "C:\Program Files\Docker\Docker"
)

$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    throw "Hay mo PowerShell bang Run as administrator roi chay lai script nay."
}

$frontendPath = Join-Path $DockerInstallPath "frontend"
$frontendExe = Join-Path $frontendPath "Docker Desktop.exe"
$backendExe = Join-Path $DockerInstallPath "resources\com.docker.backend.exe"

if (-not (Test-Path $frontendExe)) {
    throw "Khong tim thay Docker Desktop frontend tai: $frontendExe"
}

if (-not (Test-Path $backendExe)) {
    throw "Khong tim thay Docker Desktop backend tai: $backendExe"
}

Get-Process |
    Where-Object { $_.ProcessName -like "Docker Desktop*" -or $_.ProcessName -like "com.docker*" } |
    Stop-Process -Force -ErrorAction SilentlyContinue

$key = "HKLM:\SOFTWARE\Docker Inc.\Docker Desktop"
New-Item -Path $key -Force | Out-Null
New-ItemProperty -Path $key -Name AppPath -PropertyType String -Value $frontendPath -Force | Out-Null
New-ItemProperty -Path $key -Name InstallPath -PropertyType String -Value $DockerInstallPath -Force | Out-Null

Write-Host "Da tao lai registry Docker Desktop:"
Get-ItemProperty -Path $key | Select-Object AppPath, InstallPath | Format-List

Start-Process -FilePath $backendExe -WindowStyle Hidden
Start-Sleep -Seconds 5
Start-Process -FilePath $frontendExe -ArgumentList "--name=dashboard"

Write-Host "Da mo Docker Desktop bang frontend hop le. Doi 30-90 giay roi kiem tra bang: docker info"
