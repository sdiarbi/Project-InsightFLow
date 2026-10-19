import json
import os
import urllib.request
import boto3

s3_client = boto3.client('s3')
GOLD_BUCKET = os.environ.get('GOLD_BUCKET_NAME', 'insightflow-gold-bucket-sd')
LEAD_OWNER_BUCKET = "dea-lead-owner"  # Public S3 bucket from PDF spec

def lambda_handler(event, context):
    for record in event['Records']:
        # 1. Parse incoming SQS message body
        body = json.loads(record['body'])
        payload = body if isinstance(body, dict) else json.loads(body)
        if 'Message' in payload:
            payload = json.loads(payload['Message'])
            
        # Handle S3 pointer payloads if applicable
        if 's3_key' in payload and 'bucket' in payload:
            obj = s3_client.get_object(Bucket=payload['bucket'], Key=payload['s3_key'])
            payload = json.loads(obj['Body'].read().decode('utf-8'))
        
        # 2. Extract core fields from CRM Event data
        event_obj = payload.get('event', {})
        data_obj = event_obj.get('data', {})
        
        display_name = data_obj.get('display_name')
        status_label = data_obj.get('status_label')
        lead_id = event_obj.get('lead_id') or data_obj.get('id')
        date_created = event_obj.get('date_created') or data_obj.get('date_created')
        
        # 3. Lookup Lead Owner data from public S3 bucket using lead_id
        lead_email = None
        lead_owner = None
        funnel = None
        
        if lead_id:
            try:
                owner_url = f"https://{LEAD_OWNER_BUCKET}.s3.us-east-1.amazonaws.com/{lead_id}.json"
                with urllib.request.urlopen(owner_url) as response:
                    owner_data = json.loads(response.read().decode('utf-8'))
                    lead_email = owner_data.get('lead_email')
                    lead_owner = owner_data.get('lead_owner')
                    funnel = owner_data.get('funnel')
            except Exception as e:
                print(f"Warning: Could not fetch lead owner data for {lead_id}: {str(e)}")
        
        # 4. Construct Consolidated Gold Record
        enriched_record = {
            "display_name": display_name,
            "status_label": status_label,
            "lead_id": lead_id,
            "date_created": date_created,
            "lead_email": lead_email,
            "lead_owner": lead_owner,
            "funnel": funnel,
            "raw_payload": payload
        }
        
        file_key = f"gold-lead-{lead_id or context.aws_request_id}.json"
        
        # 5. Store in Gold Bucket
        s3_client.put_object(
            Bucket=GOLD_BUCKET,
            Key=file_key,
            Body=json.dumps(enriched_record),
            ContentType='application/json'
        )
        
        # 6. Format Team Notification Payload (Slack / Email integration ready)
        notification_message = (
            f"--- New Lead Alert ---\n"
            f"Name: {display_name}\n"
            f"Lead ID: {lead_id}\n"
            f"Created Date: {date_created}\n"
            f"Label: {status_label}\n"
            f"Email: {lead_email}\n"
            f"Lead Owner: {lead_owner}\n"
            f"Funnel: {funnel}"
        )
        print(notification_message)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully enriched lead with owner data and stored in Gold layer!')
    }