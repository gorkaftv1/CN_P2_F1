"""
NOTA: Este archivo ya NO se usa en el pipeline actual.
Se elimino Dynamic Partitioning de Firehose debido al limite de 500 particiones activas.
Ahora los datos se escriben sin particionar a raw/ y Glue maneja el particionado en processed/.

Mantener este archivo por si se necesita en el futuro, pero actualmente:
- ProcessingConfiguration esta deshabilitado en Firehose
- Lambda no se crea en timed_script.ps1
- Los datos fluyen directamente: Kinesis Stream -> Firehose -> S3 raw/ (sin transformacion)
"""

import json
import base64
import datetime

def lambda_handler(event, context):
    output = []
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        
        # El payload viene como string de dict, convertirlo a dict real
        data_dict = eval(payload)  # O usar ast.literal_eval para más seguridad
        
        # Extraer driverId para partición ANTES de eliminarlo del payload
        driver_id = str(data_dict['driverId'])
        
        # Eliminar driverId y positionText del payload
        # driverId estará en el path de S3 como partición
        # positionText es innecesario
        data_without_partition = {k: v for k, v in data_dict.items() if k not in ['driverId', 'positionText']}
        
        # Convertir a JSON string válido
        data_json = json.dumps(data_without_partition)
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode((data_json + '\n').encode('utf-8')).decode('utf-8'),
            'metadata': {
                'partitionKeys': {
                    'driverId': driver_id
                }
            }
        }
        output.append(output_record)
    
    return {'records': output}
