import json
import base64
import gzip
from datetime import datetime

def lambda_handler(event, context):
    output = []
    
    for record in event['records']:
        try:
            # Decode the data
            compressed_payload = base64.b64decode(record['data'])
            uncompressed_payload = gzip.decompress(compressed_payload)
            log_data = json.loads(uncompressed_payload)
            
            transformed_lines = []

            if log_data['messageType'] == 'DATA_MESSAGE':
                for log_event in log_data.get('logEvents', []):
                    try:
                        message_data = json.loads(log_event['message'])
                        transformed_record = {
                            'timestamp': datetime.fromtimestamp(log_event['timestamp'] / 1000).isoformat(),
                            'message': message_data.get('message', log_event['message']),
                            'level': message_data.get('level', 'INFO'),
                            'application_name': message_data.get('application_name', log_data['logGroup'].split('/')[-1])
                        }
                    except:
                        transformed_record = {
                            'timestamp': datetime.fromtimestamp(log_event['timestamp'] / 1000).isoformat(),
                            'message': log_event['message'],
                            'level': 'INFO',
                            'application_name': log_data['logGroup'].split('/')[-1]
                        }
                    
                    transformed_lines.append(json.dumps(transformed_record))
                
                joined_data = '\n'.join(transformed_lines) + '\n'
                output_record = {
                    'recordId': record['recordId'],  # ← 変更なし
                    'result': 'Ok',
                    'data': base64.b64encode(joined_data.encode('utf-8')).decode('utf-8')
                }
                output.append(output_record)
            else:
                output.append({
                    'recordId': record['recordId'],
                    'result': 'Ok',
                    'data': record['data']
                })
        except Exception as e:
            output.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed'
            })
    
    return {'records': output}
