import json
import os
import urllib.request
import boto3

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

BRONZE_BUCKET = os.environ.get('BRONZE_BUCKET_NAME', 'insightflow-bronze-bucket-sd')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
API_TOKEN = "0323ade64e13f79821bdc0f2a9410d9ec3873aa9df01f8a4a54d4e0f3dd2e6b4"
MEDIA_IDS = ["8hunphufxp", "9k4tbcdfg0"]

def fetch_wistia_stats(media_id):
    # Wistia Stats endpoint URL
    url = f"https://api.wistia.com/v1/stats/medias/{media_id}.json"
    
    req = urllib.request.Request(url)
    # Wistia standard auth uses Bearer token
    req.add_header('Authorization', f'Bearer {API_TOKEN}')
    
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"Error fetching Wistia stats for {media_id}: {str(e)}")
        return None

def lambda_handler(event, context):
    for media_id in MEDIA_IDS:
        data = fetch_wistia_stats(media_id)
        if data:
            s3_key = f"wistia/wistia_stats_{media_id}.json"
            
            # 1. Land raw batch payload in Bronze S3
            s3_client.put_object(
                Bucket=BRONZE_BUCKET,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            print(f"Successfully landed Wistia stats for {media_id} into Bronze S3.")
            
            # 2. Push pointer message to SQS Delay Queue for downstream Gold processing
            if SQS_QUEUE_URL:
                message_body = {
                    "media_id": media_id,
                    "s3_key": s3_key,
                    "bucket": BRONZE_BUCKET
                }
                sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(message_body)
                )
            
    return {
        'statusCode': 200,
        'body': json.dumps('Wistia batch ingestion and queuing completed successfully!')
    }