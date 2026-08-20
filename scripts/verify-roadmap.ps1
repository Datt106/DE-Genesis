param(
    [switch]$SkipTests,
    [switch]$CheckOlistData
)

$ErrorActionPreference = "Stop"

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Description,
        [Parameter(Mandatory = $true)]
        [scriptblock]$Command
    )

    Write-Host "`n==> $Description"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Description thất bại với exit code $LASTEXITCODE."
    }
}

$docker = Get-Command docker -ErrorAction SilentlyContinue
if (-not $docker) {
    throw "Không tìm thấy docker.exe. Hãy cài và khởi động Docker Desktop."
}

Invoke-Checked "Kiểm tra Docker Engine" {
    docker info --format "{{.ServerVersion}}"
}

Invoke-Checked "Kiểm tra cấu hình Docker Compose của toàn bộ profile" {
    docker compose --profile bigdata --profile streaming --profile workflow `
        --profile monitoring config --quiet
}

Invoke-Checked "Biên dịch tĩnh toàn bộ mã Python" {
    docker compose run --rm --no-deps workspace `
        python -m compileall -q dags exercises mock_api
}

Invoke-Checked "Kiểm tra cấu hình và alert rule Prometheus" {
    docker run --rm --entrypoint promtool `
        -v "${PWD}/config/prometheus:/etc/prometheus:ro" `
        prom/prometheus:v2.52.0 `
        check config /etc/prometheus/prometheus.yml
}

if ($CheckOlistData) {
    & "$PSScriptRoot/check-olist-data.ps1"
}

if (-not $SkipTests) {
    Invoke-Checked "Chạy toàn bộ kiểm thử roadmap" {
        docker compose run --rm --no-deps workspace pytest -q
    }
}

Write-Host "`nĐã xác minh xong roadmap trên môi trường Docker local."
