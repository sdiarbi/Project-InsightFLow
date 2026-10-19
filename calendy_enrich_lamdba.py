import json
import os
import boto3

s3_client = boto3.client('s3')
GOLD_BUCKET = os.environ.get('GOLD_BUCKET_NAME', 'insightflow-gold-bucket-sd')

def lambda_handler(event, context):
    records = event.get('Records', [{"body": json.dumps(event)}])
    
    for record in records:
        body = json.loads(record.get('body', json.dumps(event)))
        payload = body if isinstance(body, dict) else json.loads(body)
        
        event_uuid = payload.get('event_uuid')
        s3_key = payload.get('s3_key')
        bucket = payload.get('bucket')
        
        if s3_key and bucket:
            obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
            calendly_data = json.loads(obj['Body'].read().decode('utf-8'))
        else:
            calendly_data = payload
            
        # Extract core Calendly event and invitee attributes
        event_obj = calendly_data.get('payload', {}).get('event', {})
        invitee_obj = calendly_data.get('payload', {}).get('invitee', {})
        
        if not event_uuid:
            event_uuid = event_obj.get('uuid', 'unknown_uuid')
            
        event_name = event_obj.get('name', 'Unknown Event')
        start_time = event_obj.get('start_time')
        end_time = event_obj.get('end_time')
        invitee_email = invitee_obj.get('email', 'unknown_email')
        invitee_name = invitee_obj.get('name', 'Unknown Name')
        status = invitee_obj.get('status', 'active')
        
        gold_record = {
            "event_uuid": event_uuid,
            "event_name": event_name,
            "invitee_email": invitee_email,
            "invitee_name": invitee_name,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "raw_payload": calendly_data
        }
        
        file_key = f"gold-calendly-{event_uuid}.json"
        
        s3_client.put_object(
            Bucket=GOLD_BUCKET,
            Key=file_key,
            Body=json.dumps(gold_record),
            ContentType='application/json'
        )
        print(f"Successfully wrote Gold Calendly record for event {event_uuid}")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Calendly enrichment completed successfully!')
    }
