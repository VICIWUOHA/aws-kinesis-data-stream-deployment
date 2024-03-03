# Lambda Function to consume data from Kinesis Data stream and send to a dynamodb table.
# Author: Victor Iwuoha (https://github.com/VICIWUOHA), (https://linkedin.com/in/viciwuoha)

import json
import boto3
import base64


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    # iterate through Kinesis records and process to stream or persist in db
    for record in event["Records"]:
        raw_data = base64.b64decode(record["kinesis"]["data"])
        # get data from record
        data = json.loads(raw_data)
        partition_key = record["kinesis"]["partitionKey"]

        # put data into dynamodb
        table = dynamodb.Table("rates")
        table_data = {
            "partition_key": data["EVENT_TIME"],
            "partition_id": partition_key,
            "data": data,
        }
        table.put_item(Item=table_data)
        print(
            f"=> Successfully Witten data with partition_key => `{data['EVENT_TIME']}` to dynamoDB.."
        )
        # sample data => {"EVENT_TIME": "2024-03-02 11:31:41.536153", "TICKER": "MSFT"}

        msg = f"Processed Data Stream -> {data}"

    return {"statusCode": 200, "body": msg}
