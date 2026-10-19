import json
import os
import boto3

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

BUCKET_NAME = os.environ.get('BRONZE_BUCKET_NAME')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

def lambda_handler(event, context):
    try:
        # 1. Parse incoming body from API Gateway
        body = json.loads(event.get('body', '{}'))
        
        # 2. Extract lead_id from Close webhook payload structure
        lead_id = body.get('lead_id') or body.get('event', {}).get('lead_id') or body.get('event', {}).get('object_id')
        
        if not lead_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing lead_id in webhook payload'})
            }
            
        # 3. Define S3 file path as specified in project requirements
        s3_key = f"crm_leads/crm_event_{lead_id}.json"
        
        # 4. Save raw JSON event into S3 Bronze Layer
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(body),
            ContentType='application/json'
        )
        
        # 5. Push message to SQS Queue (queue-level 10-minute delay handles the rest)
        message_body = {
            'lead_id': lead_id,
            's3_key': s3_key,
            'bucket': BUCKET_NAME
        }
        
        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Webhook successfully ingested and queued',
                'lead_id': lead_id
            })
        }
        
    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
