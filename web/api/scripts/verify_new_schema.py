import sys
import os
import asyncio
from pprint import pprint
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load Env
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'global.env')
load_dotenv(ENV_PATH, override=True)

from meta.meta_curl import fetch_and_store as fetch_meta
from google_ads.google_sdk import fetch_and_store as fetch_google
from Database.database import DynamoDB

async def verify_sync():
    print("--- 🚀 Starting Verification Sync ---")
    
    # Trigger 7-day sync for both platforms
    print("\n[META] Syncing...")
    # fetch_and_store is sync, but we call it from here
    meta_data = fetch_meta(days=7)
    print(f"[META] Synced {len(meta_data)} items.")

    print("\n[GOOGLE] Syncing...")
    google_data = fetch_google(days=7)
    print(f"[GOOGLE] Synced {len(google_data)} items.")

    # Inspect the DB directly
    print("\n--- 🔍 Inspecting DynamoDB Data ---")
    
    for table_name in ["MetaAdsInsights", "GoogleAdsInsights"]:
        db = DynamoDB(table_name=table_name)
        await db.connect()
        
        print(f"\nTable: {table_name}")
        resp = await db.async_table.scan(Limit=5)
        items = resp.get('Items', [])
        
        if not items:
            print(f"  ❌ No data found in {table_name}")
            continue
            
        for item in items:
            pk = item.get('integration_id')
            sk = item.get('campaign_id_range')
            camp_id = item.get('campaign_id')
            days = item.get('range_days')
            print(f"  [OK] PK(integration_id): {pk}")
            print(f"       SK(campaign_id_range): {sk}")
            print(f"       Attributes: campaign_id={camp_id}, range_days={days}")
            print("-" * 20)
            
        await db.close()

if __name__ == "__main__":
    asyncio.run(verify_sync())
