import json
import os
import boto3

s3_client = boto3.client('s3')
GOLD_BUCKET = os.environ.get('GOLD_BUCKET_NAME', 'insightflow-gold-bucket-sd')

def lambda_handler(event, context):
    records = event.get('Records', [ {"body": json.dumps(event)} ])
    
    for record in records:
        body = json.loads(record.get('body', json.dumps(event)))
        payload = body if isinstance(body, dict) else json.loads(body)
        
        media_id = payload.get('media_id')
        
        if 's3_key' in payload and 'bucket' in payload:
            obj = s3_client.get_object(Bucket=payload['bucket'], Key=payload['s3_key'])
            wistia_data = json.loads(obj['Body'].read().decode('utf-8'))
        else:
            wistia_data = payload
            
        if not media_id:
            media_id = wistia_data.get('media', {}).get('hashed_id', 'unknown')
        
        # Correctly map Wistia stats fields
        load_count = wistia_data.get('load_count', 0)
        play_count = wistia_data.get('play_count', 0)
        unique_visitors = wistia_data.get('visitors', 0)
        play_rate = wistia_data.get('play_rate', 0.0)
        hours_watched = wistia_data.get('hours_watched', 0.0)
        engagement = wistia_data.get('engagement', 0.0)
        
        gold_record = {
            "media_id": media_id,
            "load_count": load_count,
            "play_count": play_count,
            "unique_visitors": unique_visitors,
            "play_rate": play_rate,
            "hours_watched": hours_watched,
            "engagement": engagement,
            "raw_payload": wistia_data
        }
        
        file_key = f"gold-wistia-{media_id}.json"
        
        s3_client.put_object(
            Bucket=GOLD_BUCKET,
            Key=file_key,
            Body=json.dumps(gold_record),
            ContentType='application/json'
        )
        print(f"Successfully wrote Gold Wistia record for media {media_id}")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Wistia enrichment completed successfully!')
    }