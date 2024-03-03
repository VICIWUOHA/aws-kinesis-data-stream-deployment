# Python  Script to put records into Kinesis Data stream
# Author: Victor Iwuoha (https://github.com/VICIWUOHA)

import os
import json
import boto3
import random
from datetime import datetime
from time import sleep
from dotenv import load_dotenv

load_dotenv(override=True)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
# Session token is optional if you don't Use SSO
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")


kinesis = boto3.client("kinesis")


def getData():
    data = {}
    now = datetime.now()
    data["EVENT_TIME"] = str(now)
    data["TICKER"] = random.choice(["AAPL", "AMZN", "MSFT", "INTC", "TBV"])
    return data


def put_record():
    partition_counter = 0
    partition = 1
    while True:
        data = json.dumps(getData())
        print(data)
        # Implement some sleep that simulates a real system.
        sleep(random.randint(1, 3))
        # partition requires a string data type and can be implemented depending on your needs
        # This can be a datetime or even another field that can help you easily retreive your data from the stream.
        response = kinesis.put_record(
            StreamName="dummy-stream-demo-vic", Data=data, PartitionKey=f"{partition}"
        )
        print(
            f"=> Successfully Inserted message with Sequence Id => `{response['SequenceNumber']}` at Partition `{partition}`"
        )
        print(f"=> Currently at Iteration.. {partition_counter}")

        partition_counter += 1
        if partition_counter == 11:
            partition += 1


if __name__ == "__main__":
    print("Putting records in stream")
    put_record()
    print("Done.")
