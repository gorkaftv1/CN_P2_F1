import boto3
import csv
import json
import sys
import time
from pathlib import Path
from loguru import logger

# CONFIGURACIÓN
STREAM_NAME = 'f1-driver-standings-stream'
REGION = 'us-east-1'  # Cambia si usas otra región
BATCH_SIZE = 100  # Reducido para evitar problemas con Firehose
BATCH_DELAY = 1.0  # Segundos de espera entre batches (aumentado para dar tiempo a Firehose)

INPUT_FILE = Path('data') / 'driver_standings_with_info.csv'

kinesis = boto3.client('kinesis', region_name=REGION)

def load_csv_data(file_path):
    """Carga los datos del CSV y los convierte en lista de diccionarios"""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data

def to_int(val, default=0):
    """Convierte cadenas numéricas a int de forma segura (acepta '10.0')."""
    try:
        if val is None or val == '':
            return default
        return int(float(val))
    except Exception:
        return default

def run_producer():
    # usar la ruta fija proporcionada por el usuario
    if not INPUT_FILE.exists():
        logger.error("Fichero no encontrado: %s", INPUT_FILE)
        logger.error("Genera el CSV con: python src/merge_driver_standings.py\nLuego ejecuta: python src/kinesis.py")
        sys.exit(1)

    data = load_csv_data(INPUT_FILE)
    total_records = len(data)
    records_sent = 0
    
    logger.info(f"Iniciando transmision al stream: {STREAM_NAME}...")
    logger.info(f"Total de registros a enviar: {total_records}")
    logger.info(f"Usando batches de {BATCH_SIZE} registros con {BATCH_DELAY}s de espera entre batches")
    
    # Procesar en batches
    for i in range(0, total_records, BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        records = []
        
        for registro in batch:
            # Estructura del mensaje a enviar
            payload = {
                'driverStandingsId': to_int(registro.get('driverStandingsId')),
                'raceId': to_int(registro.get('raceId')),
                'driverId': to_int(registro.get('driverId')),
                'points': to_int(registro.get('points')),
                'position': to_int(registro.get('position')),
                'positionText': registro.get('positionText'),
                'wins': to_int(registro.get('wins')),
                'forename': registro.get('forename'),
                'surname': registro.get('surname'),
                'dob': registro.get('dob'),
                'nationality': registro.get('nationality')
            }
            
            records.append({
                'Data': json.dumps(payload),
                'PartitionKey': str(registro['driverId'])  # Usar driverId para mejor distribucion
            })
        
        # Enviar batch a Kinesis
        try:
            response = kinesis.put_records(
                StreamName=STREAM_NAME,
                Records=records
            )
            
            # Verificar si hubo errores
            failed_count = response['FailedRecordCount']
            if failed_count > 0:
                logger.warning(f"Batch {i//BATCH_SIZE + 1}: {failed_count} registros fallaron")
            
            records_sent += len(records) - failed_count
            logger.info(f"Progreso: {records_sent}/{total_records} registros enviados ({(records_sent/total_records)*100:.1f}%)")
            
            # Esperar entre batches (excepto en el ultimo)
            if i + BATCH_SIZE < total_records:
                time.sleep(BATCH_DELAY)
            
        except Exception as e:
            logger.error(f"Error enviando batch: {e}")
            continue

    logger.success(f"Transmision completada! Total registros enviados: {records_sent}/{total_records}")
    
    if records_sent < total_records:
        logger.warning(f"Algunos registros no se enviaron: {total_records - records_sent} fallaron")

if __name__ == '__main__':
    run_producer()
