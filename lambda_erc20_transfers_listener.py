import json
import boto3

kinesis_client = boto3.client('kinesis')
dynamodb_client =  boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

erc20_transfer_event = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def get_stream_data(input_data: dict) -> dict:
    transfer_value_gwei = int(input_data["data"],16) / 1000000000
    return {"blockNumber": input_data["blockNumber"],
            "transactionHash": input_data['transactionHash'],
            "data": transfer_value_gwei,
            "topics": input_data["topics"]
    }


def push_to_kinesis(stream_name: str, data: dict) -> None:
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey="partitionkey")
    print(f"Data pushed to Kinesis: {data}")
        

def put_item_dynamodb(data: dict) -> None:
    prepared_item = {"blockNumber": {"N": str(data["blockNumber"])},
                     "transactionHash": {"S":data['transactionHash']},
                     "data": {"N":str(data['data'])},
                     "topics": {"S": str(data["topics"])}
                    }
    
    dynamodb_client.put_item(TableName='erc20_transfers', Item=prepared_item)
    print(f"Data uploaded: {prepared_item}")
    
    
def get_total_tokens() -> None:
    table = dynamodb_resource.Table('erc20_transfers')
    response = table.scan(AttributesToGet=['data'])

    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    total_tokens = sum([token['data'] for token in data])
    print(f"Total tokens transfered (Gwei): {total_tokens}")

        
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    sns_msg = event['Records'][0]["Sns"]['Message']
    txn_log = json.loads(sns_msg)
    print(f"Transaction log: {sns_msg}")
    
    topics = txn_log['topics']
    top_topic = topics[0]
    print("Top Topic: " + top_topic)
    
    if top_topic == erc20_transfer_event and txn_log["data"] != '0x' and len(topics) == 3:
        print("This is an ERC20 TRANSFER event!")

        print("Storing data to DynamoDB!")
        put_item_dynamodb(kinesis_data)
        
        get_total_tokens()
        
        print("Pushing data to Kinesis!")
        kinesis_data = get_stream_data(txn_log)
        push_to_kinesis("ethereum-block-data", kinesis_data)
       
    return True
