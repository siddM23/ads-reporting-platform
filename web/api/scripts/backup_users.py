
import os
import sys
import json
import boto3
from decimal import Decimal
from typing import Dict, Any

# Ensure path to database module
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

# Load env from root
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def backup_users():
    print("Backing up 'Users' table...")
    users_db = DynamoDB(table_name="Users")
    
    # We must initialize boto3 resource manually if not using async connect or if table object not ready
    # The DynamoDB class init does this for sync client.
    
    try:
        table = users_db.table
        if not table:
             print("Error: Table resource not initialized.")
             return

        # Scan all items
        response = table.scan()
        items = response.get('Items', [])
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
            
        print(f"Found {len(items)} users.")
        
        backup_file = 'users_backup.json'
        with open(backup_file, 'w') as f:
            json.dump(items, f, cls=DecimalEncoder, indent=2)
            
        print(f"Successfully backed up {len(items)} users to {backup_file}")
            
    except Exception as e:
        print(f"Error backing up users: {e}")

if __name__ == "__main__":
    backup_users()
