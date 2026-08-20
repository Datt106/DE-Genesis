param(
    [ValidatePattern("^[A-Za-z0-9._-]+$")]
    [string]$Topic = "service-logs-week4-lab",

    [ValidateRange(1, 1000000)]
    [int]$Count = 20,

    [ValidateRange(0, 3600)]
    [double]$IntervalSeconds = 0.2,

    [ValidateRange(0, 1000000)]
    [int]$InvalidEvery = 5,

    [switch]$Build
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$DockerCommand = Get-Command docker -ErrorAction Stop
$Docker = $DockerCommand.Source
$TopicPath = $Topic -replace "[^A-Za-z0-9._-]", "_"
$OutputRoot = "/workspace/output/week4/$TopicPath"

Push-Location $ProjectRoot
try {
    $StartParameters = @{Target = "week4"}
    if ($Build) {
        $StartParameters.Build = $true
    }
    & (Join-Path $ProjectRoot "scripts\start.ps1") @StartParameters

    & $Docker compose exec -T kafka kafka-topics `
        --bootstrap-server kafka:29092 `
        --create `
        --if-not-exists `
        --topic $Topic `
        --partitions 1 `
        --replication-factor 1
    if ($LASTEXITCODE -ne 0) {
        throw "Không thể tạo hoặc kiểm tra Kafka topic $Topic."
    }

    & $Docker compose exec -T workspace python `
        exercises/week4/kafka_producer.py `
        --topic $Topic `
        --count $Count `
        --interval $IntervalSeconds `
        --invalid-every $InvalidEvery `
        --seed 42
    if ($LASTEXITCODE -ne 0) {
        throw "Producer tuần 4 kết thúc với lỗi."
    }

    & $Docker compose exec -T spark-master `
        /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --conf spark.jars.ivy=/tmp/.ivy2 `
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
        /workspace/exercises/week4/spark_streaming_kafka.py `
        --topic $Topic `
        --starting-offsets earliest `
        --available-now `
        --window-duration "10 seconds" `
        --watermark-delay "0 seconds" `
        --output "$OutputRoot/status_report" `
        --checkpoint "$OutputRoot/checkpoint_status" `
        --quarantine-output "$OutputRoot/quarantine" `
        --quarantine-checkpoint "$OutputRoot/checkpoint_quarantine" `
        --metrics-output "$OutputRoot/quality_metrics" `
        --metrics-checkpoint "$OutputRoot/checkpoint_metrics"
    if ($LASTEXITCODE -ne 0) {
        throw "Spark Structured Streaming tuần 4 kết thúc với lỗi."
    }

    Write-Host "Đã hoàn tất pipeline tuần 4 cho topic $Topic."
    Write-Host "Đầu ra trên máy host: output/week4/$TopicPath"
    Write-Host "Các service Docker vẫn chạy để có thể kiểm tra kết quả."
}
finally {
    Pop-Location
}
