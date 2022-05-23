import json
import boto3
import time
import string
import random


erc20_approve_event = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

dynamodb_client =  boto3.client('dynamodb')
sns_client = boto3.client('sns')

SECOND_SNS_TOPIC = ""

def get_stream_data(input_data: dict) -> dict:
    return {"blockNumber": input_data["blockNumber"],
            "transactionHash": input_data['transactionHash'],
            "data": input_data["data"],
            "topics": input_data["topics"],
            # Add timestamp
            "timestamp": int(time.time())
    }


def put_item_dynamodb(data: dict) -> None:
    prepared_item = {"blockNumber": {"N": str(data["blockNumber"])},
                     "transactionHash": {"S": data['transactionHash']},
                     "data": {"S": str(data['data'])},
                     "topics": {"S": str(data["topics"])},
                     "timestamp": {"N": str(data['timestamp'])}
                    } 
    
    dynamodb_client.put_item(TableName='erc20_approvals', Item=prepared_item)
    print(f"Data uploaded: {prepared_item}")
    

def publish_to_sns(data: dict) -> None:
    sns_client.publish(Message=str(data),
                        MessageGroupId="erc20-approvals",
                        MessageDeduplicationId=''.join(random.choices(string.ascii_uppercase + string.digits, k=20)),
                        TopicArn=SECOND_SNS_TOPIC
                        )

        
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    sns_msg = event['Records'][0]["Sns"]['Message']
    txn_log = json.loads(sns_msg)
    print(f"Transaction log: {sns_msg}")
    
    topics = txn_log['topics']
    top_topic = topics[0]
    print("Top Topic: " + top_topic)
    
    if top_topic == erc20_approve_event and txn_log["data"] == '0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' and len(topics) == 3:
        print("This is an ERC20 APPROVE event!")
        prepared_data = get_stream_data(txn_log)
        
        print("Pushing data to secondary SNS pubsub!")
        publish_to_sns(prepared_data)
        
        #print("Storing data to DynamoDB!")
        #put_item_dynamodb(prepared_data)
    
    return True
