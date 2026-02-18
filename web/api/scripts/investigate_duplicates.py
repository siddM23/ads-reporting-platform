import asyncio
import os
import sys
from collections import defaultdict
from dotenv import load_dotenv

# Add parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load Env
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'global.env')
load_dotenv(ENV_PATH, override=True)

from Database.database import DynamoDB

async def investigate():
    print("--- 🔍 Investigating MetaAdsInsights for Duplicates & Inconsistencies ---")
    db = DynamoDB(table_name="MetaAdsInsights")
    await db.connect()
    
    if not db.async_table:
        print("❌ Failed to connect to DynamoDB")
        return

    print("Scanning table...")
    items = []
    response = await db.async_table.scan()
    items.extend(response.get('Items', []))
    
    while 'LastEvaluatedKey' in response:
        response = await db.async_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))
        
    print(f"Grand Total Items: {len(items)}")
    
    # Key: (integration_id, campaign_id, range_days)
    # Value: list of items
    dupes = defaultdict(list)
    
    for item in items:
        key = (item.get('integration_id'), item.get('campaign_id'), item.get('range_days'))
        dupes[key].append(item)
        
    print("\n--- Summary ---")
    duplicate_keys = {k: v for k, v in dupes.items() if len(v) > 1}
    print(f"Total Unique Keys: {len(dupes)}")
    print(f"Keys with Duplicates: {len(duplicate_keys)}")
    
    if duplicate_keys:
        print("\n--- Sample Duplicates ---")
        for k, v in list(duplicate_keys.items())[:5]:
            print(f"Key: {k}")
            for i, item in enumerate(v):
                print(f"  Version {i+1}: Name={item.get('campaign_name')}, Spend={item.get('spend')}, Synced={item.get('last_synced')}")
            print("-" * 20)
            
    # Check for items with missing integration_id
    missing_iid = [item for item in items if not item.get('integration_id')]
    print(f"\nItems with MISSING integration_id: {len(missing_iid)}")
    if missing_iid:
        print(f"  Sample: {missing_iid[0].get('campaign_name')} (ID: {missing_iid[0].get('campaign_id')})")
        
    # Check for inconsistent account_names for same integration_id
    acc_names = defaultdict(set)
    for item in items:
        if item.get('integration_id'):
            acc_names[item.get('integration_id')].add(item.get('account_name'))
            
    inconsistent_acc = {k: v for k, v in acc_names.items() if len(v) > 1}
    print(f"Integrations with multiple account_names in DB: {len(inconsistent_acc)}")
    if inconsistent_acc:
        for k, v in inconsistent_acc.items():
            print(f"  Integration {k}: {v}")

    await db.close()

if __name__ == "__main__":
    asyncio.run(investigate())
