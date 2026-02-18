import boto3
import time
import os
from dotenv import load_dotenv

# Load environment
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
load_dotenv(ENV_PATH, override=True)

# Configure DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def add_user_id_index(table_name):
    table = dynamodb.Table(table_name)
    
    # Check if index already exists
    try:
        description = table.global_secondary_indexes
        if description:
            for index in description:
                if index['IndexName'] == 'UserIdIndex':
                    print(f"Index 'UserIdIndex' already exists on {table_name}. Skipping.")
                    return
    except Exception:
        pass

    print(f"Adding UserIdIndex to {table_name}...")
    try:
        table.update(
            AttributeDefinitions=[
                {'AttributeName': 'user_id', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    'Create': {
                        'IndexName': 'UserIdIndex',
                        'KeySchema': [
                            {'AttributeName': 'user_id', 'KeyType': 'HASH'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                }
            ]
        )
        print(f"GSI creation initiated for {table_name}. Waiting for it to become ACTIVE...")
        
        while True:
            table.load()
            indices = table.global_secondary_indexes
            gsi = next((i for i in indices if i['IndexName'] == 'UserIdIndex'), None)
            
            if gsi and gsi['IndexStatus'] == 'ACTIVE':
                print(f"✅ GSI 'UserIdIndex' is now ACTIVE on {table_name}.")
                break
            else:
                status = gsi['IndexStatus'] if gsi else "UNKNOWN"
                print(f"  [{table_name}] Table: {table.table_status}, GSI: {status}")
                time.sleep(10)
    except Exception as e:
        print(f"Error adding GSI to {table_name}: {e}")

if __name__ == "__main__":
    add_user_id_index('Integrations')
