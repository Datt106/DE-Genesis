$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dataDirectory = Join-Path $projectRoot "data\olist"
$requiredFiles = @(
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
)

$missing = @(
    $requiredFiles | Where-Object {
        -not (Test-Path -LiteralPath (Join-Path $dataDirectory $_) -PathType Leaf)
    }
)

if ($missing.Count -gt 0) {
    Write-Error ("Thiếu {0} file Olist trong {1}:`n- {2}" -f $missing.Count, $dataDirectory, ($missing -join "`n- "))
}

$empty = @(
    $requiredFiles | Where-Object {
        (Get-Item -LiteralPath (Join-Path $dataDirectory $_)).Length -eq 0
    }
)
if ($empty.Count -gt 0) {
    Write-Error ("Các file Olist sau đang rỗng:`n- {0}" -f ($empty -join "`n- "))
}

Write-Host "Đã tìm thấy đủ 9 file Olist và tất cả đều có dữ liệu."
