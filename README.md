# 🏎️ F1 Driver Standings - Data Lake en AWS

**Proyecto**: Pipeline de Data Lake para datos de Fórmula 1  
**Autor**: Gorka  
**Universidad**: Cloud Computing 2025/26  
**Fecha**: Enero 2026

Pipeline completo de datos de Fórmula 1 (clasificaciones de pilotos) con ingestión en streaming y procesamiento ETL en AWS.

## 📋 Índice

1. Resumen Ejecutivo
2. Arquitectura Completa
3. Componentes AWS - Explicación Detallada
4. Estimación de Costos
5. Optimizaciones Implementadas
6. Recomendaciones

## 📋 Descripción

Sistema de Data Lake que procesa datos de clasificaciones de pilotos de F1 mediante:
- **Streaming en tiempo real**: Kinesis → Firehose → S3
- **Procesamiento ETL**: AWS Glue con Spark
- **Particionamiento**: Por `raceId` y `driverId` (manejado por Glue)
- **Catálogo de datos**: AWS Glue Data Catalog
- **Consultas**: Amazon Athena

## 🏗️ Arquitectura

```
CSV Data → Kinesis Stream → Firehose → Lambda → S3 raw/ (particionado por fecha)
                                                    ↓
                                               Glue Crawler
                                                    ↓
                                            Glue Data Catalog
                                                    ↓
                               Glue ETL Jobs → S3 processed/ (particionado por raceId/driverId)
                                                    ↓
                                                 Athena
```

**Particionamiento**:
- **Raw Zone**: Particionado automáticamente por fecha (`partition_date=YYYY-MM-DD`) mediante Lambda + Firehose Dynamic Partitioning
- **Processed Zone**: Particionado por `raceId` (tabla by_race) y sin particionar (tabla by_driver) mediante Glue Jobs

## 📁 Estructura del Proyecto

```
.
├── data/                          # Datos CSV de entrada
│   ├── driver_standings.csv      # Datos originales de clasificaciones
│   ├── drivers.csv                # Información de pilotos
│   └── driver_standings_with_info.csv  # Dataset combinado (34,863 registros)
├── scripts/
│   ├── timed_script.ps1           # Script principal con monitorización (RECOMENDADO)
│   ├── cleanup_script.ps1         # Elimina todos los recursos AWS
│   ├── athena_queries.ps1         # Ejecuta consultas Athena de prueba
│   ├── simple_script.ps1          # Script básico de despliegue
│   └── old_scripts/               # Versiones anteriores (no usar)
├── src/
│   ├── lambda/
│   │   └── firehose_driver_standings.py       # Lambda ACTIVA: transforma datos en Firehose (elimina positionText, añade partition_date)
│   ├── producer/
│   │   └── kinesis.py                         # Producer: envía datos a Kinesis (100 rec/batch, 2.1s delay)
│   ├── glue_jobs/
│   │   ├── driver_standings_aggregation_by_race.py   # Glue Job: agregación por carrera + top driver con Window functions
│   │   └── driver_standing_aggregation_by_driver.py  # Glue Job: agregación por piloto + estadísticas (sum, avg, stddev)
│   └── preprocessing/
│       ├── merge_driver_standings.py          # Preprocessing: combina driver_standings + drivers (ejecutado previamente)
│       └── generate_demo.py                   # Preprocessing: genera dataset reducido (ejecutado previamente)
└── README.md
```

## 🚀 Inicio Rápido

### Prerequisitos

- **AWS CLI** configurado con credenciales
- **Python 3.12+** con entorno virtual
- **PowerShell** (Windows) o **Bash** (Linux/Mac)
- **Rol IAM** `LabRole` con permisos necesarios

### Instalación

1. **Clonar repositorio**
```bash
git clone <repo-url>
cd P2
```

2. **Crear entorno virtual e instalar dependencias**
```bash
python -m venv .venv
# Windows
.venv\Scripts\Activate.ps1
# Linux/Mac
source .venv/bin/activate

pip install boto3 pandas loguru
```

3. **Configurar AWS (solo primera vez)**

Obtén tus credenciales de AWS Academy y ejecuta:
```powershell
# Edita scripts/configure_aws.ps1 con tus credenciales
.\scripts\configure_aws.ps1
```

### Despliegue Automático

**Windows (PowerShell) - RECOMENDADO:**
```powershell
cd scripts
.\timed_script.ps1 -JobToRun both
```

**Parámetros disponibles:**
- `-JobToRun both` (por defecto): Ejecuta ambos jobs
- `-JobToRun race`: Solo ejecuta agregación por carrera
- `-JobToRun driver`: Solo ejecuta agregación por piloto

**Script alternativo (básico):**
```powershell
cd scripts
.\simple_script.ps1
```

El script `timed_script.ps1` realiza automáticamente:
1. ✅ Creación de bucket S3 y carpetas
2. ✅ Creación de Kinesis Stream (monitorea hasta ACTIVE)
3. ✅ Creación de Lambda y configuración de Firehose con Dynamic Partitioning por fecha
4. ✅ Creación de Glue Database y Crawlers (2: raw + processed)
5. ✅ Subida de scripts ETL a S3
6. ✅ Creación de Glue Jobs (2 jobs)
7. ✅ Ejecución del productor Kinesis (34,863 registros en batches de 100)
8. ✅ Espera monitorizada de Firehose (30s después de completar)
9. ✅ Ejecución y monitoreo de Crawler RAW
10. ✅ Ejecución y monitoreo de Glue Jobs
11. ✅ Ejecución y monitoreo de Crawler procesado
12. ✅ Consultas Athena de prueba

**Duración estimada:** 8-15 minutos (depende del tamaño de datos)

## 📊 Recursos Creados

### S3
- **Bucket**: `datalake-f1-driverstandings-{ACCOUNT_ID}`
- **Estructura**:
  - `raw/f1_driver_standings/partition_date=YYYY-MM-DD/` - Datos JSONL particionados por fecha
  - `processed/driver_standings_by_race/raceId=X/` - Parquet particionado por carrera
  - `processed/driver_standings_by_driver/` - Parquet sin particiones (datos agregados)
  - `scripts/` - Scripts ETL
  - `errors/` - Errores de Firehose (si los hay)
  - `logs/` - Logs de Spark
  - `queries/` - Archivos temporales de queries

### Kinesis
- **Stream**: `f1-driver-standings-stream` (1 shard)
- **Throughput**: 1 MB/s escritura, 2 MB/s lectura

### Firehose
- **Delivery Stream**: `f1-driver-standings-delivery-stream`
- **Source**: Kinesis Stream
- **Destination**: S3 raw/ (con transformación Lambda)
- **Buffering**: 64 MB / 60 segundos
- **Lambda Processor**: Elimina campo `positionText`, añade metadata `partition_date`
- **Dynamic Partitioning**: Particionado por fecha (`partition_date=YYYY-MM-DD`)

### Lambda
- **Function**: `firehose-driver-standings-lambda`
- **Runtime**: Python 3.10
- **Memory**: 128 MB
- **Timeout**: 60 segundos
- **Transformación**: Elimina campos redundantes y añade fecha UTC para particionamiento

### Glue
- **Database**: `f1_db`
- **Crawlers**:
  - `f1-driver-standings-raw-crawler` → Tabla: `f1_driver_standings`
  - `f1-driver-standings-processed-crawler` → Tablas: `driver_standings_by_race`, `driver_standings_by_driver`
- **Jobs**:
  - `driver-standings-by-race` (Glue 4.0, 2x G.1X workers)
  - `driver-standings-by-driver` (Glue 4.0, 2x G.1X workers)

## 🔍 Consultas con Athena

Una vez completado el despliegue, puedes consultar los datos en Athena:

```sql
-- Ver datos raw
SELECT * FROM f1_db.f1_driver_standings LIMIT 10;

-- Ver datos procesados por carrera
SELECT * FROM f1_db.driver_standings_by_race LIMIT 10;

-- Ver datos procesados por piloto
SELECT * FROM f1_db.driver_standings_by_driver LIMIT 10;
```

## 🛠️ Componentes Principales

### `producer/kinesis.py`
Productor que lee `driver_standings_with_info.csv` (34,863 registros) y envía al stream de Kinesis.
- **Batch size**: 100 registros por llamada
- **Delay**: 1 segundo entre batches
- **Duración**: ~5-7 minutos para enviar todos los datos

### `lambda/firehose_driver_standings.py`
**Lambda ACTIVA** - Función de transformación en Firehose.
- **Función**: Elimina campo `positionText` (redundante, derivado de `position`)
- **Particionamiento**: Añade `partition_date` (fecha UTC) a metadata para Dynamic Partitioning
- **Ventaja**: Solo genera ~1-30 particiones por fecha (vs 1,100+ por raceId), evita límite de 500
- **Ejecución**: < 50ms por lote de 100 registros

### `glue_jobs/driver_standings_aggregation_by_race.py`
Job Glue/Spark que:
- Lee datos de `f1_db.f1_driver_standings`
- Agrega por `raceId`
- Calcula: total_drivers, total_points, top_driver (ganador), etc.
- **Escribe particionado** por `raceId` en formato Parquet

### `glue_jobs/driver_standing_aggregation_by_driver.py`
Job Glue/Spark que:
- Lee datos de `f1_db.f1_driver_standings`
- Agrega por `driverId`
- Calcula: mejor_posicion_historica, posicion_promedio, desviacion_estandar, total_carreras
- **Escribe particionado** por `driverId` en formato Parquet

## ⚙️ Configuración

### Variables de entorno (scripts)
- `AWS_REGION`: `us-east-1`
- `ACCOUNT_ID`: Obtenido automáticamente
- `BUCKET_NAME`: `datalake-f1-driverstandings-{ACCOUNT_ID}`
- `ROLE_ARN`: ARN del rol `LabRole`

### Glue Jobs
- **Versión**: 4.0
- **Workers**: 2
- **Worker Type**: G.1X
- **Python Version**: 3

## 🔐 Seguridad

- ❌ **NO subir** `configure_aws.ps1` (contiene credenciales)
- ❌ **NO subir** carpeta `.venv/`
- ❌ **NO subir** carpeta `old_scripts/`
- ✅ Usar `.gitignore` para excluir archivos sensibles
- ✅ Credenciales temporales de AWS Academy (expiran en horas)

## 📝 Notas Importantes

- **Dynamic Partitioning por fecha**: Particiona raw/ por `partition_date` (evita límite de 500 con ~30 fechas únicas)
- **Lambda ligera**: Transformación simple (< 50ms), elimina 1 campo y añade metadata de fecha
- **Particionamiento multi-nivel**: raw/ por fecha (temporal), processed/ por raceId/driverId (entidad de negocio)
- **Batch size reducido**: 100 registros para evitar saturar Firehose
- **Monitorización activa**: El script espera a que cada recurso esté listo antes de continuar
- **Credenciales temporales**: AWS Academy requiere renovación periódica
- **Jobs secuenciales**: El segundo job espera a que el primero complete
- **Cleanup completo**: `cleanup_script.ps1` elimina TODOS los recursos (usar con cuidado)

## 🐛 Troubleshooting

### Error: "DynamicPartitioning.ActivePartitionsLimitExceeded"
**Prevención**: Resuelto mediante particionamiento por fecha (< 30 particiones) en lugar de raceId (> 1,100). Si aparece, verificar que Lambda esté añadiendo correctamente `partition_date` a metadata.

### Error: "Bucket already exists"
**Solución**: Ejecutar `.\cleanup_script.ps1` primero para eliminar recursos existentes.

### Error: "Crawler still running"
**Solución**: El script ya incluye monitorización. Si persiste, esperar manualmente a que el crawler termine.

### No hay datos en S3 raw/
**Causas posibles**:
1. Kinesis producer falló → Revisar logs en terminal
2. Firehose no está ACTIVE → Verificar estado con AWS CLI
3. Buffering de Firehose → Esperar hasta 60s para que escriba

### Jobs Glue fallan
**Causas posibles**:
1. Tabla `f1_driver_standings` no existe → Ejecutar crawler raw primero
2. Permisos del rol LabRole → Verificar permisos S3/Glue
3. Path incorrecto en scripts → Verificar output_path en job args

### Cleanup no elimina todo
**Solución**: Algunos recursos pueden tardar en eliminarse (Firehose ~30s). Ejecutar cleanup dos veces si es necesario.

## 👥 Autores

- Gorka - Cloud Computing - Universidad 2025/26

## 📄 Licencia

Proyecto académico - Universidad 2025/26
