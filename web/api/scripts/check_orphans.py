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

async def check_orphans():
    print("--- 🔍 Checking for Orphaned Metrics (MetaAdsInsights) ---")
    
    # 1. Fetch all integrations (including deleted ones)
    int_db = DynamoDB(table_name="Integrations")
    await int_db.connect()
    
    print("Fetching integrations...")
    all_ints_resp = await int_db.async_table.scan()
    all_ints = all_ints_resp.get('Items', [])
    while 'LastEvaluatedKey' in all_ints_resp:
        all_ints_resp = await int_db.async_table.scan(ExclusiveStartKey=all_ints_resp['LastEvaluatedKey'])
        all_ints.extend(all_ints_resp.get('Items', []))
        
    int_map = {i['id']: i for i in all_ints}
    active_ids = {i['id'] for i in all_ints if not i.get('is_deleted')}
    deleted_ids = {i['id'] for i in all_ints if i.get('is_deleted')}
    
    print(f"Total Integrations in DB: {len(all_ints)}")
    print(f"  Active: {len(active_ids)}")
    print(f"  Deleted: {len(deleted_ids)}")

    # 2. Fetch all metrics
    met_db = DynamoDB(table_name="MetaAdsInsights")
    await met_db.connect()
    
    print("\nScanning metrics...")
    met_resp = await met_db.async_table.scan()
    metrics = met_resp.get('Items', [])
    while 'LastEvaluatedKey' in met_resp:
        met_resp = await met_db.async_table.scan(ExclusiveStartKey=met_resp['LastEvaluatedKey'])
        metrics.extend(met_resp.get('Items', []))
        
    print(f"Total Metric Rows: {len(metrics)}")
    
    # Analyze
    orphans = []
    deleted_linked = []
    
    for m in metrics:
        iid = m.get('integration_id')
        if not iid:
            orphans.append(m)
        elif iid not in int_map:
            orphans.append(m)
        elif iid in deleted_ids:
            deleted_linked.append(m)
            
    print(f"\nMetric counts:")
    print(f"  Tied to Active: {len(metrics) - len(orphans) - len(deleted_linked)}")
    print(f"  Tied to Deleted: {len(deleted_linked)}")
    print(f"  Truly Orphaned (No matching Integration): {len(orphans)}")
    
    if deleted_linked:
        print("\n--- Sample Metrics tied to DELETED integrations ---")
        for m in deleted_linked[:5]:
            i = int_map[m['integration_id']]
            print(f"  Campaign: {m.get('campaign_name')} (ID: {m.get('campaign_id')})")
            print(f"  Account: {m.get('account_name')} | Platform: {i.get('platform')} | Deleted logic: {i.get('is_deleted')}")
            print("-" * 20)
            
    if orphans:
        print("\n--- Sample Truly Orphaned Metrics ---")
        for m in orphans[:5]:
            print(f"  Campaign: {m.get('campaign_name')} (ID: {m.get('campaign_id')}) | Linked ID: {m.get('integration_id')}")

    await int_db.close()
    await met_db.close()

if __name__ == "__main__":
    asyncio.run(check_orphans())
