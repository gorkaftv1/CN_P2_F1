import boto3
import csv
import time
import sys
from pathlib import Path
from loguru import logger

# CONFIGURACIÓN
STREAM_NAME = 'f1-driver-standings-stream'
REGION = 'us-east-1'  # Cambia si usas otra región

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
    records_sent = 0
    
    logger.info(f"Iniciando transmisión al stream: {STREAM_NAME}...")
    logger.info(f"Total de registros a enviar: {len(data)}")
    
    for registro in data:
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
        
        # Enviar a Kinesis usando raceId como clave de partición
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=str(payload).replace("'", '"'),  # Convertir a formato JSON
            PartitionKey=registro['raceId']  # Usamos raceId como clave de partición
        )
        
        records_sent += 1
        
        if records_sent % 100 == 0:  # Log cada 100 registros
            logger.info(f"Progreso: {records_sent}/{len(data)} registros enviados")
        
        # Pequeña pausa para simular streaming y no saturar de golpe
        time.sleep(0.05)  # 50ms entre registros

    logger.info(f"Fin de la transmisión. Total registros enviados: {records_sent}")

if __name__ == '__main__':
    run_producer()
