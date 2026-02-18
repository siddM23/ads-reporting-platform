
import boto3
import os
import sys

# Add path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv

# Load env variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

def verify_table_count(table_name):
    print(f"Verifying actual count for table: {table_name}")
    try:
        dynamodb = boto3.resource('dynamodb', region_name=os.getenv("AWS_REGION", "us-east-1"))
        table = dynamodb.Table(table_name)
        
        response = table.scan(Select='COUNT')
        count = response.get('Count', 0)
        
        # Handle pagination if more items exist
        while 'LastEvaluatedKey' in response:
            response = table.scan(Select='COUNT', ExclusiveStartKey=response['LastEvaluatedKey'])
            count += response.get('Count', 0)
            
        print(f"  Start Scan Count: {count}")
        return count
            
    except Exception as e:
        print(f"  Error scanning table {table_name}: {e}")
        return 0
    print("-" * 20)

if __name__ == "__main__":
    users_count = verify_table_count("Users")
    integrations_count = verify_table_count("Integrations")
    
    if users_count > 0 and integrations_count > 0:
        print("\n✅ Verification Successful: Data restored.")
    else:
        print("\n⚠️  Verification Warning: Some tables might be empty.")
