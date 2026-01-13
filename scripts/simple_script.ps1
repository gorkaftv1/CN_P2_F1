# Parámetros del script
param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("race", "driver", "both")]
    [string]$JobToRun = "both"
)

# Variables
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$SrcDir = Join-Path $ProjectRoot "src"
$ScriptsDir = Join-Path $ProjectRoot "scripts"

$AWS_REGION = "us-east-1"
$ACCOUNT_ID = aws sts get-caller-identity --query Account --output text
$BUCKET_NAME = "datalake-f1-driverstandings-$ACCOUNT_ID"
$ROLE_ARN = aws iam get-role --role-name LabRole --query 'Role.Arn' --output text

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "F1 Driver Standings - Setup Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Bucket: $BUCKET_NAME" -ForegroundColor Yellow
Write-Host "Role: $ROLE_ARN" -ForegroundColor Yellow
Write-Host "Project Root: $ProjectRoot" -ForegroundColor Yellow
Write-Host "Job a ejecutar: $JobToRun" -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Cyan

# S3
Write-Host "[1/11] Creando S3 Bucket y carpetas..." -ForegroundColor Green
aws s3 mb s3://$BUCKET_NAME

aws s3api put-object --bucket $BUCKET_NAME --key raw/
aws s3api put-object --bucket $BUCKET_NAME --key raw/f1_driver_standings/
aws s3api put-object --bucket $BUCKET_NAME --key processed/
aws s3api put-object --bucket $BUCKET_NAME --key processed/driver_standings_by_race/
aws s3api put-object --bucket $BUCKET_NAME --key processed/driver_standings_by_driver/
aws s3api put-object --bucket $BUCKET_NAME --key config/
aws s3api put-object --bucket $BUCKET_NAME --key scripts/
aws s3api put-object --bucket $BUCKET_NAME --key queries/
aws s3api put-object --bucket $BUCKET_NAME --key errors/
aws s3api put-object --bucket $BUCKET_NAME --key logs/

# Kinesis
Write-Host "[2/11] Creando Kinesis Stream..." -ForegroundColor Green
aws kinesis create-stream --stream-name f1-driver-standings-stream --shard-count 1

# Lambda
Write-Host "[3/11] Creando Lambda Function..." -ForegroundColor Green
Set-Location $SrcDir
if (Test-Path firehose_driver_standings.zip) { Remove-Item firehose_driver_standings.zip }
Compress-Archive -Path firehose_driver_standings.py -DestinationPath firehose_driver_standings.zip -Force

aws lambda create-function `
    --function-name f1-firehose-lambda `
    --runtime python3.10 `
    --role $ROLE_ARN `
    --handler firehose_driver_standings.lambda_handler `
    --zip-file fileb://firehose_driver_standings.zip `
    --timeout 60

Remove-Item firehose_driver_standings.zip

$LAMBDA_ARN = aws lambda get-function --function-name f1-firehose-lambda --query 'Configuration.FunctionArn' --output text

Set-Location $ScriptsDir
Write-Host "[4/11] Creando Firehose Delivery Stream..." -ForegroundColor Green

# Firehose
$firehoseConfig = @{
    BucketARN = "arn:aws:s3:::$BUCKET_NAME"
    RoleARN = $ROLE_ARN
    Prefix = "raw/f1_driver_standings/driverId=!{partitionKeyFromLambda:driverId}/"
    ErrorOutputPrefix = "errors/!{firehose:error-output-type}/"
    BufferingHints = @{ SizeInMBs = 64; IntervalInSeconds = 60 }
    CompressionFormat = "UNCOMPRESSED"
    ProcessingConfiguration = @{
        Enabled = $true
        Processors = @(
            @{
                Type = "Lambda"
                Parameters = @(
                    @{ ParameterName = "LambdaArn"; ParameterValue = $LAMBDA_ARN }
                    @{ ParameterName = "BufferSizeInMBs"; ParameterValue = "1" }
                    @{ ParameterName = "BufferIntervalInSeconds"; ParameterValue = "60" }
                )
            },
            @{
                Type = "AppendDelimiterToRecord"
                Parameters = @()
            }
        )
    }
    DynamicPartitioningConfiguration = @{ Enabled = $true }
}

$firehoseConfig | ConvertTo-Json -Depth 10 | Set-Content firehose_config.json

aws firehose create-delivery-stream `
    --delivery-stream-name f1-driver-standings-delivery-stream `
    --delivery-stream-type KinesisStreamAsSource `
    --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:${AWS_REGION}:${ACCOUNT_ID}:stream/f1-driver-standings-stream,RoleARN=$ROLE_ARN" `
    --extended-s3-destination-configuration file://firehose_config.json

Remove-Item firehose_config.json

Write-Host "[5/11] Creando Glue Database..." -ForegroundColor Green
# Glue Database
$dbInput = @{ Name = "f1_db" } | ConvertTo-Json
$dbInput | Set-Content glue_db_input.json
aws glue create-database --database-input file://glue_db_input.json
Remove-Item glue_db_input.json
Write-Host "[6/11] Creando Glue Crawler..." -ForegroundColor Green

# Glue Crawler
$crawlerTargets = @{ S3Targets = @( @{ Path = "s3://$BUCKET_NAME/raw/f1_driver_standings" } ) }
$crawlerTargets | ConvertTo-Json -Depth 10 | Set-Content crawler_targets.json

aws glue create-crawler `
    --name f1-driver-standings-raw-crawler `
    --role $ROLE_ARN `
    --database-name f1_db `
    --targets file://crawler_targets.json

Remove-Item crawler_targets.json

# Crawler para processed/driver_standings_by_race
$crawlerTargets = @{ S3Targets = @( @{ Path = "s3://$BUCKET_NAME/processed/driver_standings_by_race" } ) }
$crawlerTargets | ConvertTo-Json -Depth 10 | Set-Content crawler_targets.json

aws glue create-crawler `
    --name f1-driver-standings-by-race-crawler `
    --role $ROLE_ARN `
    --database-name f1_db `
    --targets file://crawler_targets.json

Remove-Item crawler_targets.json

# Crawler para processed/driver_standings_by_driver
$crawlerTargets = @{ S3Targets = @( @{ Path = "s3://$BUCKET_NAME/processed/driver_standings_by_driver" } ) }
$crawlerTargets | ConvertTo-Json -Depth 10 | Set-Content crawler_targets.json

aws glue create-crawler `
    --name f1-driver-standings-by-driver-crawler `
    --role $ROLE_ARN `
    --database-name f1_db `
    --targets file://crawler_targets.json

Remove-Item crawler_targets.json
Write-Host "[7/11] Subiendo scripts ETL a S3..." -ForegroundColor Green

# Subir scripts ETL
Set-Location $SrcDir
aws s3 cp standings_aggregation_by_race.py s3://$BUCKET_NAME/scripts/
aws s3 cp driver_standing_aggregation_by_race.py s3://$BUCKET_NAME/scripts/
aws s3 cp driver_standing_aggregation_by_driver.py s3://$BUCKET_NAME/scripts/
Write-Host "[8/11] Creando Glue Jobs..." -ForegroundColor Green
Set-Location $ScriptsDir

# Glue Jobs
$DATABASE = "f1_db"
$TABLE = "f1_driver_standings"

$commandConfig = @{ Name = "glueetl"; ScriptLocation = "s3://$BUCKET_NAME/scripts/standings_aggregation_by_race.py"; PythonVersion = "3" }
$defaultArgs = @{ "--database" = $DATABASE; "--table" = $TABLE; "--output_path" = "s3://$BUCKET_NAME/processed/driver_standings_by_race/"; "--enable-continuous-cloudwatch-log" = "true"; "--spark-event-logs-path" = "s3://$BUCKET_NAME/logs/" }
$commandConfig | ConvertTo-Json | Set-Content job_command.json
$defaultArgs | ConvertTo-Json | Set-Content job_args.json

aws glue create-job `
    --name driver-standings-by-race `
    --role $ROLE_ARN `
    --command file://job_command.json `
    --default-arguments file://job_args.json `
    --glue-version "4.0" `
    --number-of-workers 2 `
    --worker-type "G.1X"

Remove-Item job_command.json, job_args.json

$commandConfig = @{ Name = "glueetl"; ScriptLocation = "s3://$BUCKET_NAME/scripts/driver_standing_aggregation_by_driver.py"; PythonVersion = "3" }
$defaultArgs = @{ "--database" = $DATABASE; "--table" = $TABLE; "--output_path" = "s3://$BUCKET_NAME/processed/driver_standings_by_driver/"; "--enable-continuous-cloudwatch-log" = "true"; "--spark-event-logs-path" = "s3://$BUCKET_NAME/logs/" }
$commandConfig | ConvertTo-Json | Set-Content job_command.json
$defaultArgs | ConvertTo-Json | Set-Content job_args.json

aws glue create-job `
    --name driver-standings-by-driver `
    --role $ROLE_ARN `
    --command file://job_command.json `
    --default-arguments file://job_args.json `
    --glue-version "4.0" `
    --number-of-workers 2 `
    --worker-type "G.1X"

Remove-Item job_command.json, job_args.json

# Ejecutar productor Kinesis
Write-Host "[9/11] Ejecutando productor Kinesis..." -ForegroundColor Green
Set-Location $ProjectRoot
if (Test-Path ".venv\Scripts\python.exe") {
    & .venv\Scripts\python.exe src\kinesis.py
} else {
    & python src\kinesis.py
}

# Verificar que el productor terminó correctamente
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: El productor Kinesis falló con código de salida $LASTEXITCODE" -ForegroundColor Red
    exit 1
}
Write-Host "Productor Kinesis completado exitosamente" -ForegroundColor Green

# Esperar a que los datos lleguen a S3
Write-Host "[10/11] Esperando 90 segundos para que Firehose procese datos..." -ForegroundColor Green
Start-Sleep -Seconds 90

# Ejecutar Crawler
Write-Host "[10/11] Ejecutando Crawler..." -ForegroundColor Green
aws glue start-crawler --name f1-driver-standings-raw-crawler

Write-Host "Esperando 60 segundos para que el crawler termine..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

# Ejecutar Jobs de Glue
Write-Host "[11/11] Ejecutando Glue Jobs..." -ForegroundColor Green

if ($JobToRun -eq "race" -or $JobToRun -eq "both") {
    # Primer Job
    Write-Host "`nLanzando job: driver-standings-by-race" -ForegroundColor Yellow
    $jobRun1 = aws glue start-job-run --job-name driver-standings-by-race | ConvertFrom-Json
    $runId1 = $jobRun1.JobRunId
    Write-Host "Job iniciado con Run ID: $runId1" -ForegroundColor Cyan

    # Monitorear primer job
    Write-Host "Monitoreando ejecucion del job..." -ForegroundColor Yellow
    $maxWait = 600  # 10 minutos máximo
    $waited = 0
    while ($waited -lt $maxWait) {
        $jobStatus = (aws glue get-job-run --job-name driver-standings-by-race --run-id $runId1 | ConvertFrom-Json).JobRun.JobRunState
        if ($jobStatus -in @("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT")) {
            Write-Host "Job terminado con estado: $jobStatus" -ForegroundColor $(if($jobStatus -eq "SUCCEEDED"){"Green"}else{"Red"})
            break
        }
        Write-Host "  Estado: $jobStatus (esperando...)" -ForegroundColor Gray
        Start-Sleep -Seconds 15
        $waited += 15
    }

    if ($waited -ge $maxWait) {
        Write-Host "Timeout esperando al job. Continuando..." -ForegroundColor DarkYellow
    }
    
    # Si el job terminó exitosamente, ejecutar el crawler para by_race
    if ($jobStatus -eq "SUCCEEDED") {
        Write-Host "`nEjecutando crawler para driver_standings_by_race..." -ForegroundColor Yellow
        aws glue start-crawler --name f1-driver-standings-by-race-crawler
        Write-Host "Crawler f1-driver-standings-by-race-crawler iniciado" -ForegroundColor Green
    }
}

if ($JobToRun -eq "driver" -or $JobToRun -eq "both") {
    # Segundo Job
    Write-Host "`nLanzando job: driver-standings-by-driver" -ForegroundColor Yellow
    $jobRun2 = aws glue start-job-run --job-name driver-standings-by-driver | ConvertFrom-Json
    $runId2 = $jobRun2.JobRunId
    Write-Host "Job iniciado con Run ID: $runId2" -ForegroundColor Cyan

    if ($JobToRun -eq "driver") {
        # Si solo ejecutamos este job, monitorearlo
        Write-Host "Monitoreando ejecucion del job..." -ForegroundColor Yellow
        $maxWait = 600
        $waited = 0
        while ($waited -lt $maxWait) {
            $jobStatus = (aws glue get-job-run --job-name driver-standings-by-driver --run-id $runId2 | ConvertFrom-Json).JobRun.JobRunState
            if ($jobStatus -in @("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT")) {
                Write-Host "Job terminado con estado: $jobStatus" -ForegroundColor $(if($jobStatus -eq "SUCCEEDED"){"Green"}else{"Red"})
                break
            }
            Write-Host "  Estado: $jobStatus (esperando...)" -ForegroundColor Gray
            Start-Sleep -Seconds 15
            $waited += 15
        }
        
        # Si el job terminó exitosamente, ejecutar el crawler para by_driver
        if ($jobStatus -eq "SUCCEEDED") {
            Write-Host "`nEjecutando crawler para driver_standings_by_driver..." -ForegroundColor Yellow
            aws glue start-crawler --name f1-driver-standings-by-driver-crawler
            Write-Host "Crawler f1-driver-standings-by-driver-crawler iniciado" -ForegroundColor Green
        }
    } else {
        # Si ejecutamos both, también lanzar el crawler
        Write-Host "`nEjecutando crawler para driver_standings_by_driver..." -ForegroundColor Yellow
        aws glue start-crawler --name f1-driver-standings-by-driver-crawler
        Write-Host "Crawler f1-driver-standings-by-driver-crawler iniciado" -ForegroundColor Green
    }
}

# Ver estado de los jobs
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "SETUP COMPLETADO" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "`nEstado de los jobs:" -ForegroundColor Yellow

if ($JobToRun -eq "race" -or $JobToRun -eq "both") {
    aws glue get-job-runs --job-name driver-standings-by-race --max-items 1
}

if ($JobToRun -eq "driver" -or $JobToRun -eq "both") {
    aws glue get-job-runs --job-name driver-standings-by-driver --max-items 1
}

Write-Host "`nPara probar Athena, ejecuta: .\scripts\athena_queries.ps1" -ForegroundColor Cyan
