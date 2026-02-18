import sys
import os
import json
import uuid
import time
import datetime
from decimal import Decimal
from dotenv import load_dotenv

# Add parent directory
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Load environment variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

from Database.database import DynamoDB

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def migrate():
    print("Starting Integrations Table Migration...")
    db = DynamoDB(table_name="Integrations")
    
    # 1. Backup
    print("\n--- Phase 1: Backup ---")
    try:
        # Note: list_integrations in updated database.py filters is_deleted=False
        # But for migration, we might want EVERYTHING even if we previously had soft delete (we didn't).
        # We can use db.table.scan() direct.
        if db.table:
            print("Scanning existing integrations...")
            response = db.table.scan()
            items = response.get('Items', [])
            print(f"Found {len(items)} items.")
            
            backup_file = "integrations_backup.json"
            with open(backup_file, "w") as f:
                json.dump(items, f, cls=DecimalEncoder, indent=2)
            print(f"✅ Backup saved to {backup_file}")
        else:
            print("Table does not exist. Skipping backup.")
            items = []
            
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        # If backup fails, DO NOT PROCEED
        return

    # 2. Drop Table
    print("\n--- Phase 2: Drop Old Table ---")
    try:
        if db.table:
            db.table.delete()
            print("Waiting for table deletion...")
            db.table.wait_until_not_exists()
            print("✅ Table deleted.")
    except Exception as e:
        if "ResourceNotFoundException" in str(e):
            print("Table already gone.")
        else:
            print(f"❌ Delete failed: {e}")
            return

    # 3. Recreate Table
    # New Schema: PK=id (uuid), No SK. GSIs: EmailIndex, PlatformAccountIndex.
    print("\n--- Phase 3: Create New Table ---")
    try:
        # create_table(pk='id', sk=None)
        db.create_table(pk='id', sk=None)
        print("✅ Base table created.")
        
        # Create GSIs
        db.create_integrations_gsis()
        
        # Wait for table to be ACTIVE
        print("Waiting for table to be active...")
        db.table.meta.client.get_waiter('table_exists').wait(TableName="Integrations")
        
    except Exception as e:
        print(f"❌ Creation failed: {e}")
        return

    # 4. Restore & Transform
    print("\n--- Phase 4: Restore Data ---")
    if not items:
        print("No items to restore.")
        return

    restored_count = 0
    for item in items:
        try:
            # Generate new UUID PK
            new_id = str(uuid.uuid4())
            
            # Prepare new item
            new_item = {
                'id': new_id,
                'is_deleted': False, # Default new field
                'updated_at': datetime.datetime.utcnow().isoformat()
            }
            
            # Copy old fields
            # Old PK was platform, SK was account_id. They are now just attributes.
            for k, v in item.items():
                if k not in new_item:
                    new_item[k] = v
            
            # Clean Decimal to float/int if needed, or Boto3 handles Decimals fine on put.
            # actually Boto3 handles Decimal input.
            
            db.table.put_item(Item=new_item)
            restored_count += 1
            print(f"Restored: {new_item.get('platform')} - {new_item.get('account_name')}")
            
        except Exception as e:
            print(f"Failed to restore item {item.get('account_id')}: {e}")

    print(f"\n✅ Migration Complete. Restored {restored_count} integrations.")

if __name__ == "__main__":
    migrate()
