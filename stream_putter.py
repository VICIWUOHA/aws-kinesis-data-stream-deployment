import os
import boto3
import json
import time
from dotenv import load_dotenv

load_dotenv(override=True)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
# Session token is optional if you don't Use SSO
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

# Replace 'stream_name' with the name of your Kinesis Data Stream created on AWS
stream_name = "demo-stream-analytics-vic"

# Create a Kinesis client
kinesis_client = boto3.client("kinesis")


def put_record_to_stream(data):
    # Convert the data to JSON format
    data_json = json.dumps(data)

    # Put the record to the Kinesis Data Stream
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=data_json,
        PartitionKey=str(
            time.time()
        ),  # Use a unique partition key for each record (timestamp)
    )
    # log response for review later.
    with open("aws-kinesis-streaming-deploy/response.json", "w") as file:
        file.write(json.dumps(response, indent=4))
    # Print the response, including the sequence number
    print(f"Record successfully sent. Sequence number: {response['SequenceNumber']}")


# Example data to be sent as records
record_data_1 = {"device_id": 1, "temperature": 25.5, "humidity": 60.2}
record_data_2 = {"device_id": 2, "temperature": 23.1, "humidity": 58.8}

# Send two records
put_record_to_stream(record_data_1)
put_record_to_stream(record_data_2)
