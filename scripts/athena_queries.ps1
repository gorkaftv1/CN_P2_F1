# Script para ejecutar queries de Athena
# Parametros del script
param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("race", "driver", "both")]
    [string]$JobToRun = "both"
)

# Variables
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$AWS_REGION = "us-east-1"
$ACCOUNT_ID = aws sts get-caller-identity --query Account --output text
$BUCKET_NAME = "datalake-f1-driverstandings-$ACCOUNT_ID"
$DATABASE = "f1_db"

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "F1 Driver Standings - Athena Queries" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Database: $DATABASE" -ForegroundColor Yellow
Write-Host "Resultados en: s3://$BUCKET_NAME/queries/" -ForegroundColor Yellow
Write-Host "Job seleccionado: $JobToRun" -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Cyan

# Funcion para ejecutar query y esperar resultados
function Execute-AthenaQuery {
    param(
        [string]$QueryString,
        [string]$QueryName
    )
    
    Write-Host "`n[$QueryName]" -ForegroundColor Yellow
    Write-Host "Ejecutando query..." -ForegroundColor Gray
    
    # Guardar query en archivo temporal sin BOM
    $queryFile = "temp_query_$([guid]::NewGuid().ToString()).sql"
    $QueryString | Out-File -FilePath $queryFile -Encoding ASCII -NoNewline
    
    $execution = aws athena start-query-execution `
        --query-string file://$queryFile `
        --query-execution-context "Database=$DATABASE" `
        --result-configuration "OutputLocation=s3://$BUCKET_NAME/queries/" | ConvertFrom-Json
    
    Remove-Item $queryFile -ErrorAction SilentlyContinue
    
    if (-not $execution.QueryExecutionId) {
        Write-Host "Error ejecutando query" -ForegroundColor Red
        return
    }
    
    $executionId = $execution.QueryExecutionId
    Write-Host "Query ID: $executionId" -ForegroundColor Cyan
    
    # Esperar a que termine
    $maxWait = 60
    $waited = 0
    while ($waited -lt $maxWait) {
        $status = (aws athena get-query-execution --query-execution-id $executionId | ConvertFrom-Json).QueryExecution.Status.State
        
        if ($status -eq "SUCCEEDED") {
            Write-Host "Query completada exitosamente" -ForegroundColor Green
            $resultPath = "s3://$BUCKET_NAME/queries/$executionId.csv"
            Write-Host "Resultados: $resultPath" -ForegroundColor Gray
            
            # Descargar y mostrar primeras lineas
            $localFile = "query_result_$executionId.csv"
            aws s3 cp $resultPath $localFile --quiet
            if (Test-Path $localFile) {
                Write-Host "`nPrimeras lineas del resultado:" -ForegroundColor Cyan
                Get-Content $localFile -First 15 | ForEach-Object { Write-Host "  $_" -ForegroundColor White }
                Remove-Item $localFile
            }
            break
        }
        elseif ($status -in @("FAILED", "CANCELLED")) {
            Write-Host "Query fallo con estado: $status" -ForegroundColor Red
            break
        }
        
        Start-Sleep -Seconds 2
        $waited += 2
    }
    
    if ($waited -ge $maxWait) {
        Write-Host "Timeout esperando resultado" -ForegroundColor DarkYellow
    }
}

Set-Location $ScriptDir

# Query 1: Top 10 pilotos por puntos totales (tabla raw)
$query1 = "SELECT driverId, forename, surname, SUM(points) as total_points, COUNT(DISTINCT raceId) as num_races FROM f1_db.f1_driver_standings GROUP BY driverId, forename, surname ORDER BY total_points DESC LIMIT 10"

# Query 2: Estadisticas por carrera (tabla raw)
$query2 = "SELECT raceId, COUNT(DISTINCT driverId) as num_drivers, SUM(points) as total_points, AVG(points) as avg_points FROM f1_db.f1_driver_standings GROUP BY raceId ORDER BY raceId LIMIT 10"

# Query 3: Pilotos con mas victorias
$query3 = "SELECT forename, surname, nationality, MAX(wins) as total_wins FROM f1_db.f1_driver_standings GROUP BY forename, surname, nationality HAVING MAX(wins) > 0 ORDER BY total_wins DESC LIMIT 10"

# Query 4: Top pilotos por puntos agregados (si existe tabla by_driver)
$query4 = "SELECT driverId, forename, surname, total_points, avg_points, num_races FROM f1_db.driver_standings_by_driver ORDER BY total_points DESC LIMIT 10"

# Query 5: Carreras con mas participantes (si existe tabla by_race)
$query5 = "SELECT raceId, total_drivers, total_points, avg_points FROM f1_db.driver_standings_by_race ORDER BY total_drivers DESC LIMIT 10"

# Ejecutar queries comunes (siempre se ejecutan)
Execute-AthenaQuery -QueryString $query1 -QueryName "Query 1: Top 10 pilotos por puntos totales (raw)"
Execute-AthenaQuery -QueryString $query2 -QueryName "Query 2: Estadisticas por carrera (raw)"
Execute-AthenaQuery -QueryString $query3 -QueryName "Query 3: Pilotos con mas victorias"

# Ejecutar queries segun el parametro JobToRun
if ($JobToRun -eq "driver" -or $JobToRun -eq "both") {
    Write-Host "`n--- Query en tabla procesada by_driver ---" -ForegroundColor DarkYellow
    Execute-AthenaQuery -QueryString $query4 -QueryName "Query 4: Top pilotos (tabla by_driver)"
}

if ($JobToRun -eq "race" -or $JobToRun -eq "both") {
    Write-Host "`n--- Query en tabla procesada by_race ---" -ForegroundColor DarkYellow
    Execute-AthenaQuery -QueryString $query5 -QueryName "Query 5: Carreras con mas participantes (tabla by_race)"
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Queries completadas" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
$queriesPath = "s3://$BUCKET_NAME/queries/"
Write-Host "Todos los resultados estan en: $queriesPath" -ForegroundColor Yellow
Write-Host "Puedes listarlos con el comando aws s3 ls $queriesPath" -ForegroundColor Gray