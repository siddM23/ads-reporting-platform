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

def recreate_metrics_table(table_name):
    print(f"--- Recreating {table_name} with Tenant-First PK ---")
    
    # 1. Delete Table
    try:
        table = dynamodb.Table(table_name)
        print(f"Deleting existing table {table_name}...")
        table.delete()
        table.wait_until_not_exists()
        print(f"✅ Table {table_name} deleted.")
    except Exception as e:
        if "ResourceNotFoundException" in str(e):
            print(f"Table {table_name} does not exist. Proceeding to create.")
        else:
            print(f"Error deleting {table_name}: {e}")
            return

    # 2. Create Table
    # PK: integration_id (S)
    # SK: campaign_id_range (S) - Composite campaign_id#range_days
    try:
        print(f"Creating new table {table_name}...")
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'integration_id', 'KeyType': 'HASH'},
                {'AttributeName': 'campaign_id_range', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'integration_id', 'AttributeType': 'S'},
                {'AttributeName': 'campaign_id_range', 'AttributeType': 'S'},
                {'AttributeName': 'range_days', 'AttributeType': 'N'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'RangeDaysIndex',
                    'KeySchema': [
                        {'AttributeName': 'range_days', 'KeyType': 'HASH'},
                        {'AttributeName': 'campaign_id_range', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.wait_until_exists()
        print(f"✅ Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating {table_name}: {e}")

if __name__ == "__main__":
    recreate_metrics_table('MetaAdsInsights')
    recreate_metrics_table('GoogleAdsInsights')
