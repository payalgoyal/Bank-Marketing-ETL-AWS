import boto3
import pandas as pd
import json
import time

#AWS Details
stream_name = 'bank-stream'
region_name = 'ap-south-1'

kinesis_client = boto3.client('kinesis', region_name=region_name)

df = pd.read_csv('bank.csv')

#send each row as json to Kinesis stream
for index, row in df.iterrows():
    data = row.to_dict()
    data_json = json.dumps(data)
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=data_json,
        PartitionKey=str(index)  # Using index as partition key
    )

    print(f"Sent record {index + 1}: {data_json}")
    time.sleep(0.5)  # optional delay to simulate streaming