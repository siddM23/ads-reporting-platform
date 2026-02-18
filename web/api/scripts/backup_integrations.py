
import sys
import os
import json
import decimal
from datetime import datetime
import boto3

# Add parent directory to path to import Database.database
# Assumes this script is in web/api/scripts/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Database.database import DynamoDB
from dotenv import load_dotenv

# Helper class to convert DynamoDB items to JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o) # Convert Decimal to string to avoid float precision issues or just string for safety
        return super(DecimalEncoder, self).default(o)

def backup():
    # Load env
    # Path to global.env
    ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH, override=True)
    else:
        print(f"Warning: .env file not found at {ENV_PATH}")

    table_name = os.getenv("DYNAMODB_TABLE", "Integrations")
    print(f"Backing up table '{table_name}'...")
    
    db = DynamoDB(table_name=table_name)
    
    # Check if table exists
    try:
        db.table.load()
    except Exception as e:
        print(f"Error accessing table '{table_name}': {e}")
        print("Please ensure your AWS credentials and region are set correctly.")
        return

    # Scan all items
    try:
        response = db.table.scan()
        items = response.get('Items', [])
        
        while 'LastEvaluatedKey' in response:
            response = db.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        print(f"Found {len(items)} items in '{table_name}'.")

        # Save to root folder CUBE-ARP/integrations_backup.json
        # Go up 3 levels from web/api/scripts -> web/api -> web -> CUBE-ARP
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        backup_file = os.path.join(root_dir, 'integrations_backup.json')
        
        with open(backup_file, 'w') as f:
            json.dump(items, f, cls=DecimalEncoder, indent=2)
            
        print(f"✅ Backup saved successfully to: {backup_file}")
        
    except Exception as e:
        print(f"❌ Error during backup: {e}")

if __name__ == "__main__":
    backup()
