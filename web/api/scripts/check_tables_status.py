
import boto3
import os
import sys
from pprint import pprint

# Add path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv

# Load env variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

def check_table(table_name):
    print(f"Checking table: {table_name}")
    try:
        dynamodb = boto3.resource('dynamodb', region_name=os.getenv("AWS_REGION", "us-east-1"))
        table = dynamodb.Table(table_name)
        
        # Force a refresh of the table description
        table.load()
        
        print(f"  Table Status: {table.table_status}")
        print(f"  Item Count: {table.item_count}")
        print(f"  Key Schema: {table.key_schema}")
        
        if table.global_secondary_indexes:
            print("  GSIs:")
            for gsi in table.global_secondary_indexes:
                print(f"    - Name: {gsi['IndexName']}")
                print(f"      Status: {gsi['IndexStatus']}")
                print(f"      Key Schema: {gsi['KeySchema']}")
                # print(f"      Projection: {gsi['Projection']}")
        else:
            print("  No GSIs found.")
            
    except Exception as e:
        print(f"  Error checking table {table_name}: {e}")
    print("-" * 20)

if __name__ == "__main__":
    check_table("Users")
    check_table("Integrations")
