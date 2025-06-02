import json
import boto3
import base64
import uuid
from datetime import datetime

s3 = boto3.client('s3')
bucket_name = 'bank-marketing-bronze'
bronze_prefix = 'bronze/'

def lambda_handler(event,handler):
    print("Event received:", json.dumps(event))  # ✅ log full event

    for record in event['Records']:
        try:
            # Step 1: Get the base64 string
            base64_data = record['kinesis']['data']

            # Step 2: Decode base64 to bytes
            decoded_bytes = base64.b64decode(base64_data)

            # Step 3: Convert bytes to string (JSON)
            decoded_str = decoded_bytes.decode('utf-8')
            print("Decoded payload:", decoded_str)

            # Create a unique filename
            timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
            filename = f"{bronze_prefix}{timestamp}_{uuid.uuid4().hex}.json"

            # Write to S3
            s3.put_object(Bucket=bucket_name, Key=filename, Body=decoded_str)
            print(f"Written to S3: {filename}")  # ✅ log S3 path
        
        except Exception as e:
            print("Error occurred:", str(e))  # ✅ log errors
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data written to S3')
    }