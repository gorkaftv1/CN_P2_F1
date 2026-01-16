import json
import base64
from datetime import datetime

def lambda_handler(event, context):
    output = []
    # Obtener fecha actual en formato YYYY-MM-DD
    partition_date = datetime.utcnow().strftime('%Y-%m-%d')
    
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        
        # El payload viene como string de dict, convertirlo a dict real
        data_dict = json.loads(payload)
        
        # Eliminar positionText (campo redundante)
        data_without_redundant = {k: v for k, v in data_dict.items() if k not in ['positionText']}
        
        # Convertir a JSON string válido
        data_json = json.dumps(data_without_redundant)
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode((data_json + '\n').encode('utf-8')).decode('utf-8'),
            'metadata': {
                'partitionKeys': {
                    'partition_date': partition_date
                }
            }
        }
        output.append(output_record)
    
    return {'records': output}
