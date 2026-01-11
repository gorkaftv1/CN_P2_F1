# Script de limpieza - Elimina todos los recursos creados por simple_script.ps1

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

$AWS_REGION = "us-east-1"
$ACCOUNT_ID = aws sts get-caller-identity --query Account --output text
$BUCKET_NAME = "datalake-f1-driverstandings-$ACCOUNT_ID"

Write-Host "`n========================================" -ForegroundColor Red
Write-Host "SCRIPT DE LIMPIEZA - F1 Data Lake" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red
Write-Host "Bucket a eliminar: $BUCKET_NAME" -ForegroundColor Yellow
Write-Host "Account ID: $ACCOUNT_ID" -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Red

Write-Host "ADVERTENCIA: Este script eliminara TODOS los recursos creados." -ForegroundColor Yellow
$confirm = Read-Host "¿Estas seguro? (escribe 'SI' para continuar)"
if ($confirm -ne "SI") {
    Write-Host "Operacion cancelada." -ForegroundColor Green
    exit
}

# 1. Eliminar Glue Jobs
Write-Host "`n[1/8] Eliminando Glue Jobs..." -ForegroundColor Cyan
try {
    aws glue delete-job --job-name driver-standings-by-race
    Write-Host "  Job 'driver-standings-by-race' eliminado" -ForegroundColor Green
} catch {
    Write-Host "  Job 'driver-standings-by-race' no existe o ya fue eliminado" -ForegroundColor DarkYellow
}

try {
    aws glue delete-job --job-name driver-standings-by-driver
    Write-Host "  Job 'driver-standings-by-driver' eliminado" -ForegroundColor Green
} catch {
    Write-Host "  Job 'driver-standings-by-driver' no existe o ya fue eliminado" -ForegroundColor DarkYellow
}

# 2. Eliminar Glue Crawler
Write-Host "`n[2/8] Eliminando Glue Crawler..." -ForegroundColor Cyan
try {
    aws glue delete-crawler --name f1-driver-standings-raw-crawler
    Write-Host "  Crawler eliminado" -ForegroundColor Green
} catch {
    Write-Host "  Crawler no existe o ya fue eliminado" -ForegroundColor DarkYellow
}

# 3. Eliminar Glue Database (y todas sus tablas)
Write-Host "`n[3/8] Eliminando Glue Database..." -ForegroundColor Cyan
try {
    aws glue delete-database --name f1_db
    Write-Host "  Database 'f1_db' eliminada" -ForegroundColor Green
} catch {
    Write-Host "  Database no existe o ya fue eliminada" -ForegroundColor DarkYellow
}

# 4. Eliminar Firehose Delivery Stream
Write-Host "`n[4/8] Eliminando Firehose Delivery Stream..." -ForegroundColor Cyan
try {
    aws firehose delete-delivery-stream --delivery-stream-name f1-driver-standings-delivery-stream
    Write-Host "  Firehose eliminado (puede tardar unos minutos en completarse)" -ForegroundColor Green
    Start-Sleep -Seconds 30
} catch {
    Write-Host "  Firehose no existe o ya fue eliminado" -ForegroundColor DarkYellow
}

# 5. Eliminar Lambda Function
Write-Host "`n[5/8] Eliminando Lambda Function..." -ForegroundColor Cyan
try {
    aws lambda delete-function --function-name f1-firehose-lambda
    Write-Host "  Lambda eliminada" -ForegroundColor Green
} catch {
    Write-Host "  Lambda no existe o ya fue eliminada" -ForegroundColor DarkYellow
}

# 6. Eliminar Kinesis Stream
Write-Host "`n[6/8] Eliminando Kinesis Stream..." -ForegroundColor Cyan
try {
    aws kinesis delete-stream --stream-name f1-driver-standings-stream
    Write-Host "  Kinesis Stream eliminado" -ForegroundColor Green
} catch {
    Write-Host "  Kinesis Stream no existe o ya fue eliminado" -ForegroundColor DarkYellow
}

# 7. Vaciar y eliminar S3 Bucket
Write-Host "`n[7/8] Vaciando y eliminando S3 Bucket..." -ForegroundColor Cyan
try {
    Write-Host "  Vaciando bucket..." -ForegroundColor Yellow
    aws s3 rm s3://$BUCKET_NAME --recursive
    Write-Host "  Eliminando bucket..." -ForegroundColor Yellow
    aws s3 rb s3://$BUCKET_NAME
    Write-Host "  Bucket eliminado" -ForegroundColor Green
} catch {
    Write-Host "  Error al eliminar bucket (puede que no exista)" -ForegroundColor DarkYellow
}

# 8. Limpiar archivos temporales locales
Write-Host "`n[8/8] Limpiando archivos temporales locales..." -ForegroundColor Cyan
Set-Location $ProjectRoot
$tempFiles = @(
    "src\firehose_driver_standings.zip",
    "scripts\firehose_config.json",
    "scripts\glue_db_input.json",
    "scripts\crawler_targets.json",
    "scripts\job_command.json",
    "scripts\job_args.json"
)

foreach ($file in $tempFiles) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "  Eliminado: $file" -ForegroundColor Green
    }
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "LIMPIEZA COMPLETADA" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`nTodos los recursos han sido eliminados." -ForegroundColor Yellow
Write-Host "Nota: Algunos recursos como Firehose pueden tardar unos minutos en eliminarse completamente.`n" -ForegroundColor Gray
