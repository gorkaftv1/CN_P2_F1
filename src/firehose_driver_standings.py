import json
import base64
import datetime

def lambda_handler(event, context):
    output = []
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        
        # El payload viene como string de dict, convertirlo a dict real
        data_dict = eval(payload)  # O usar ast.literal_eval para más seguridad
        
        # Convertir a JSON string válido
        data_json = json.dumps(data_dict)
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode((data_json + '\n').encode('utf-8')).decode('utf-8'),
            'metadata': {
                'partitionKeys': {
                    'raceId': str(data_dict['raceId']),
                    'driverId': str(data_dict['driverId'])
                }
            }
        }
        output.append(output_record)
    
    return {'records': output}
