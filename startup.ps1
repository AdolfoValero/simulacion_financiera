$ErrorActionPreference = "Stop"

Write-Host "Starting postgres + kafka..." -ForegroundColor Green
docker compose up -d postgres kafka
Start-Sleep -Seconds 5

Write-Host "Waiting for postgres..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0
while ($attempt -lt $maxAttempts) {
    try {
        docker compose exec postgres pg_isready -U hive -d hive_metastore > $null 2>&1
        Write-Host "OK postgres healthy" -ForegroundColor Green
        break
    }
    catch {
        Write-Host "  postgres not ready yet..." -ForegroundColor Gray
        Start-Sleep -Seconds 2
        $attempt++
    }
}

Write-Host "Starting hive_metastore..." -ForegroundColor Green
docker compose up -d hive_metastore
Start-Sleep -Seconds 5

Write-Host "Waiting for hive_metastore..." -ForegroundColor Yellow
$attempt = 0
while ($attempt -lt $maxAttempts) {
    try {
        docker compose exec hive_metastore nc -z localhost 9083 > $null 2>&1
        Write-Host "OK hive_metastore healthy" -ForegroundColor Green
        break
    }
    catch {
        Write-Host "  hive_metastore not ready yet..." -ForegroundColor Gray
        Start-Sleep -Seconds 2
        $attempt++
    }
}

Write-Host "Starting trino..." -ForegroundColor Green
docker compose up -d trino
Start-Sleep -Seconds 5

Write-Host "Waiting for trino..." -ForegroundColor Yellow
$attempt = 0
while ($attempt -lt $maxAttempts) {
    try {
        docker compose exec trino curl -sf http://localhost:8080/v1/info > $null 2>&1
        Write-Host "OK trino healthy" -ForegroundColor Green
        break
    }
    catch {
        Write-Host "  trino not ready yet..." -ForegroundColor Gray
        Start-Sleep -Seconds 2
        $attempt++
    }
}

Write-Host "Starting spark_etl..." -ForegroundColor Green
docker compose up -d spark_etl
Write-Host "  (Spark takes 30s to start Jupyter)" -ForegroundColor Gray

Write-Host "Starting bi_superset..." -ForegroundColor Green
docker compose up -d bi_superset
Write-Host "  (Superset takes 30s to initialize)" -ForegroundColor Gray

Write-Host "Starting sql_scheduler..." -ForegroundColor Green
docker compose up -d sql_scheduler

Write-Host "Starting optional services..." -ForegroundColor Green
docker compose up -d ml_training ml_produccion r_streaming

Write-Host ""
Write-Host "All services started. Checking status..." -ForegroundColor Green
docker compose ps
