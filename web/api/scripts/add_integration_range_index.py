
import os
import sys
import time
import boto3

# Add path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

# Load env variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

def add_gsi(table_name):
    print(f"\n--- Adding IntegrationRangeIndex to {table_name} ---")
    db = DynamoDB(table_name=table_name)
    
    # Check if table exists
    try:
        table_desc = db.table.meta.client.describe_table(TableName=table_name)
    except Exception as e:
        print(f"Error describing table {table_name}: {e}")
        return

    existing_gsis = table_desc.get('Table', {}).get('GlobalSecondaryIndexes', [])
    if any(gsi['IndexName'] == 'IntegrationRangeIndex' for gsi in existing_gsis):
        print(f"✅ GSI 'IntegrationRangeIndex' already exists on {table_name}")
        return

    # Create GSI
    print(f"Creating GSI 'IntegrationRangeIndex' (PK: integration_id, SK: range_days) on {table_name}...")
    try:
        db.table.update(
            AttributeDefinitions=[
                {'AttributeName': 'integration_id', 'AttributeType': 'S'},
                {'AttributeName': 'range_days', 'AttributeType': 'N'}
            ],
            GlobalSecondaryIndexUpdates=[{
                'Create': {
                    'IndexName': 'IntegrationRangeIndex',
                    'KeySchema': [
                        {'AttributeName': 'integration_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'range_days', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                }
            }]
        )
        print(f"GSI creation initiated for {table_name}. Waiting for it to become ACTIVE...")
        
        while True:
            desc = db.table.meta.client.describe_table(TableName=table_name)
            status = desc['Table']['TableStatus']
            
            gsi_status = 'UNKNOWN'
            for g in desc['Table'].get('GlobalSecondaryIndexes', []):
                if g['IndexName'] == 'IntegrationRangeIndex':
                    gsi_status = g['IndexStatus']
                    break
            
            print(f"  [{table_name}] Table: {status}, GSI: {gsi_status}")
            
            if status == 'ACTIVE' and gsi_status == 'ACTIVE':
                print(f"✅ GSI 'IntegrationRangeIndex' is active on {table_name}.")
                break
            
            time.sleep(10)
            
    except Exception as e:
        print(f"❌ Failed to create GSI on {table_name}: {e}")

if __name__ == "__main__":
    add_gsi("MetaAdsInsights")
    add_gsi("GoogleAdsInsights")
    print("\nAll tasks finished.")
