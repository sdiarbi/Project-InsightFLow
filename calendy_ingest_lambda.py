import json
import os
import boto3

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

BRONZE_BUCKET = os.environ.get('BRONZE_BUCKET_NAME', 'insightflow-bronze-bucket-sd')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

def lambda_handler(event, context):
    try:
        # Handle API Gateway or Function URL body payload
        body = event.get('body', json.dumps(event))
        payload = json.loads(body) if isinstance(body, str) else body
        
        # Extract event URI or unique identifier from Calendly payload
        event_info = payload.get('event', {})
        event_uuid = payload.get('payload', {}).get('event', {}).get('uuid', 'unknown_event')
        
        s3_key = f"calendly/calendly_event_{event_uuid}.json"
        
        # 1. Land raw webhook payload in Bronze S3
        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload),
            ContentType='application/json'
        )
        print(f"Successfully landed Calendly raw payload {event_uuid} into Bronze S3.")
        
        # 2. Push pointer message to SQS Queue for downstream processing
        if SQS_QUEUE_URL:
            message_body = {
                "event_uuid": event_uuid,
                "s3_key": s3_key,
                "bucket": BRONZE_BUCKET
            }
            sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            
        return {
            'statusCode': 200,
            'body': json.dumps('Calendly webhook received and queued successfully!')
        }
        
    except Exception as e:
        print(f"Error processing Calendly webhook: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }