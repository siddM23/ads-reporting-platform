import boto3
import os
import sys
from dotenv import load_dotenv

# Add parent directory to path to find other modules if needed
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Load environment variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)
else:
    print(f"Warning: .env file not found at {ENV_PATH}")

def clear_integrations():
    table_name = "Integrations"
    
    # Initialize DynamoDB resource
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    
    table = dynamodb.Table(table_name)
    
    print(f"🧹 Starting to clear all items from {table_name}...")
    
    try:
        # Scan the table to get primary keys for deletion
        # Partition Key: platform, Sort Key: account_id
        projection_expression = "platform, account_id"
        response = table.scan(ProjectionExpression=projection_expression)
        items = response.get('Items', [])
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ProjectionExpression=projection_expression,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
        
        if not items:
            print(f"✨ Table {table_name} is already empty.")
            return

        print(f"📦 Found {len(items)} integrations to remove.")
        
        # Batch delete items (max 25 per request)
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(
                    Key={
                        'platform': item['platform'],
                        'account_id': item['account_id']
                    }
                )
        
        print(f"✅ Successfully cleared {len(items)} items from {table_name}.")
        
    except Exception as e:
        print(f"❌ Error clearing table: {e}")

if __name__ == "__main__":
    clear_integrations()
