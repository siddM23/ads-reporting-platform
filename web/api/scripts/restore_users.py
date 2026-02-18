
import os
import sys
import json
import boto3
import uuid
from decimal import Decimal
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

def restore_users_with_migration():
    print("Restoring 'Users' table with migration to UUID...")
    
    # 1. Read backup
    backup_file = 'users_backup.json'
    if not os.path.exists(backup_file):
        print(f"Error: Backup file {backup_file} not found.")
        return

    with open(backup_file, 'r') as f:
        users = json.load(f)
        
    print(f"Loaded {len(users)} users from backup.")

    # 2. Connect to DB
    # Note: We assume the table has been recreated with PK=id, SK=None (or maybe we keep SK?)
    # The plan was to change PK to id. GSI EmailIndex will be created.
    users_db = DynamoDB(table_name="Users")
    table = users_db.table
    
    if not table:
        print("Error: Table resource not initialized.")
        return

    # 3. Migrate and Restore
    success_count = 0
    with table.batch_writer() as batch:
        for user in users:
            # Generate new UUID if not present (legacy users wont have it)
            if 'id' not in user:
                user['id'] = str(uuid.uuid4())
                print(f"  Generated UUID for {user.get('email')}: {user['id']}")
            
            # Ensure proper types (Decimal back to int/float if needed, but boto3 handles int/float in put_item usually)
            # The backup JSON has ints/floats.
            
            # Remove any keys that might conflict or we want to clean up?
            # Existing schema had 'email' as PK, 'created_at' as SK.
            # New schema: 'id' as PK. 'email' is just an attribute (indexed).
            # We keep 'created_at' as attribute.
            
            batch.put_item(Item=user)
            success_count += 1
            
    print(f"Successfully restored {success_count} users to new schema.")
    
    # Trigger GSI creation if needed? 
    # The setup_db.py or database.py logic should handle GSI creation separate or we call it.
    users_db.create_users_gsis()

if __name__ == "__main__":
    restore_users_with_migration()
