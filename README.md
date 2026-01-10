# 🏎️ F1 Driver Standings - Data Lake en AWS

Pipeline completo de datos de Fórmula 1 (clasificaciones de pilotos) con ingestión en streaming y procesamiento ETL en AWS.

## 📋 Descripción

Sistema de Data Lake que procesa datos de clasificaciones de pilotos de F1 mediante:
- **Streaming en tiempo real**: Kinesis → Firehose → S3
- **Procesamiento ETL**: AWS Glue con Spark
- **Particionamiento dinámico**: Por `raceId` y `driverId`
- **Catálogo de datos**: AWS Glue Data Catalog
- **Consultas**: Amazon Athena

## 🏗️ Arquitectura

```
CSV Data → Kinesis Stream → Firehose (+ Lambda) → S3 (particionado)
                                                      ↓
                                                  Glue Crawler
                                                      ↓
                                              Glue Data Catalog
                                                      ↓
                                                 Glue ETL Jobs → S3 (procesado)
                                                      ↓
                                                   Athena
```

## 📁 Estructura del Proyecto

```
.
├── data/                          # Datos CSV de entrada
│   ├── driver_standings.csv
│   ├── drivers.csv
│   └── driver_standings_with_info.csv
├── scripts/
│   ├── simple_script.ps1         # Script de despliegue para Windows
│   ├── script.sh                 # Script de despliegue para Linux/Mac
│   └── configure_aws.ps1         # Configuración de credenciales AWS (no subir a git)
├── src/
│   ├── firehose_driver_standings.py           # Lambda para Firehose
│   ├── kinesis.py                             # Productor de datos a Kinesis
│   ├── merge_driver_standings.py              # Preparación de datos
│   ├── standings_aggregation_by_race.py       # Job Glue: agregación por carrera
│   ├── driver_standing_aggregation_by_race.py
│   └── driver_standing_aggregation_by_driver.py # Job Glue: agregación por piloto
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

**Windows (PowerShell):**
```powershell
cd scripts
.\simple_script.ps1
```

**Linux/Mac (Bash):**
```bash
cd scripts
./script.sh
```

El script realizará automáticamente:
1. ✅ Creación de bucket S3 y carpetas
2. ✅ Configuración de Kinesis Stream
3. ✅ Despliegue de Lambda Function
4. ✅ Configuración de Firehose Delivery Stream
5. ✅ Creación de Glue Database y Crawler
6. ✅ Subida de scripts ETL a S3
7. ✅ Creación de Glue Jobs
8. ✅ Ejecución del productor Kinesis
9. ✅ Ejecución del Crawler (espera 90s + 60s)
10. ✅ Ejecución secuencial de Jobs Glue

**Duración estimada:** 5-10 minutos

## 📊 Recursos Creados

### S3
- **Bucket**: `datalake-f1-driverstandings-{ACCOUNT_ID}`
- **Estructura**:
  - `raw/f1_driver_standings/` - Datos sin procesar
  - `processed/driver_standings_by_race/` - Agregación por carrera
  - `processed/driver_standings_by_driver/` - Agregación por piloto
  - `scripts/` - Scripts ETL
  - `errors/` - Errores de Firehose

### Kinesis
- **Stream**: `f1-driver-standings-stream` (1 shard)

### Lambda
- **Function**: `f1-firehose-lambda`
- **Runtime**: Python 3.10
- **Propósito**: Transformación y particionamiento dinámico

### Firehose
- **Delivery Stream**: `f1-driver-standings-delivery-stream`
- **Particionamiento**: `raceId` y `driverId`
- **Buffering**: 64 MB / 60 segundos

### Glue
- **Database**: `f1_db`
- **Crawler**: `f1-driver-standings-raw-crawler`
- **Jobs**:
  - `driver-standings-by-race`
  - `driver-standings-by-driver`

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

### `kinesis.py`
Productor que lee `driver_standings_with_info.csv` y envía registros al stream de Kinesis.

### `firehose_driver_standings.py`
Lambda que procesa registros de Firehose:
- Decodifica datos en base64
- Añade claves de partición (`raceId`, `driverId`)
- Re-encodea para S3

### `standings_aggregation_by_race.py`
Job Glue/Spark que:
- Lee datos del catálogo
- Agrega por `raceId` y `driverId`
- Calcula métricas (puntos totales, victorias, etc.)
- Guarda en S3 formato Parquet

### `driver_standing_aggregation_by_driver.py`
Job Glue/Spark que:
- Agrega datos por piloto
- Calcula estadísticas de carrera
- Guarda resultados procesados

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

## 📝 Notas

- Las credenciales de AWS Academy son temporales y deben renovarse periódicamente
- Los Jobs de Glue se ejecutan **secuencialmente** (el segundo espera al primero)
- El Crawler espera 60 segundos para catalogar datos antes de ejecutar jobs
- Firehose espera 90 segundos para procesar y guardar datos en S3

## 🐛 Troubleshooting

### Error: "Bucket already exists"
El script falla si los recursos ya existen. Usar `script.ps1` en `old_scripts/` para versión con verificaciones.

### Error: "Lambda execution role"
Verificar que el rol `LabRole` tiene permisos necesarios.

### Error: "Firehose timeout"
Aumentar los tiempos de espera en el script si es necesario.

## 👥 Autores

- Gorka - Cloud Computing - Universidad 2025/26

## 📄 Licencia

Proyecto académico - Universidad 2025/26
