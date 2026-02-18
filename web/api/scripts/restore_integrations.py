
import os
import sys
import json
import uuid
from decimal import Decimal

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

def restore_integrations():
    print("Restoring 'Integrations' table from backup...")
    
    backup_file = 'integrations_backup.json'
    if not os.path.exists(backup_file):
        # Look in parent dir too
        backup_file = '../integrations_backup.json'
        if not os.path.exists(backup_file):
            # look in CWD
            backup_file = os.path.join(os.getcwd(), 'integrations_backup.json')
            if not os.path.exists(backup_file):
                print(f"Error: Backup file 'integrations_backup.json' not found.")
                return

    with open(backup_file, 'r') as f:
        integrations = json.load(f)
        
    print(f"Loaded {len(integrations)} integrations from backup.")

    db = DynamoDB(table_name="Integrations")
    table = db.table
    
    if not table:
        print("Error: Table resource not initialized.")
        return

    success_count = 0
    with table.batch_writer() as batch:
        for item in integrations:
            # Generate UUID if missing (old schema used platform+account_id as composite key logic but stored as items... wait)
            # Old schema had id as PK? Yes, setup_db.py showed pk='id' for Integrations previously too.
            # But just in case, ensure 'id' is present.
            if 'id' not in item:
                 item['id'] = str(uuid.uuid4())
                 
            # Clean up potential float/Decimal issues coming from JSON
            # JSON has floats, DynamoDB needs Decimals or Strings. Boto3 handles float -> Decimal automatically usually,
            # but sometimes explicit conversion helps.
            
            batch.put_item(Item=item)
            success_count += 1
            
    print(f"Successfully restored {success_count} integrations.")
    
    # Trigger GSI creation if needed (should be covered by setup_db.py)
    db.create_integrations_gsis()

if __name__ == "__main__":
    restore_integrations()
