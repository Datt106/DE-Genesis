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

    throw "Không tìm thấy docker.exe. Hãy cài Docker Desktop hoặc mở PowerShell mới sau khi cài."
}

$docker = Resolve-Docker

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Đã tạo .env từ .env.example"
}

& $docker info --format "{{.ServerVersion}}" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Engine chưa sẵn sàng. Hãy mở Docker Desktop và chờ engine khởi động xong."
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

Write-Host "Đang khởi động môi trường $Target..."
& $docker @composeArgs
if ($LASTEXITCODE -ne 0) {
    throw "Không thể khởi động môi trường $Target. Hãy xem log Docker Compose ở phía trên."
}

Write-Host "Đã gửi lệnh khởi động. Dùng 'docker compose ps' để theo dõi health check."
