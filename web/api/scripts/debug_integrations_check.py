import sys
import os
import asyncio
from pprint import pprint
from dotenv import load_dotenv

# Add parent directory to path to import Database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load Env
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), 'global.env')
print(f"Loading env from {ENV_PATH}")
load_dotenv(ENV_PATH, override=True)

from Database.database import DynamoDB

async def debug_integrations(user_id=None, email=None):
    db = DynamoDB(table_name="Integrations")
    await db.connect() # Initialize async (though we can use sync too)
    
    print(f"--- Debugging Integrations ---")
    if user_id:
        print(f"Searching for user_id: {user_id}")
    if email:
        print(f"Searching for email: {email}")

    # 1. Scan everything to see what is there
    print("\n--- All Items in Table (First Page) ---")
    all_items = await db.async_table.scan()
    items = all_items.get('Items', [])
    for item in items:
        uid = item.get('user_id', 'MISSING')
        plat = item.get('platform')
        acc = item.get('account_id')
        em = item.get('email')
        print(f"Item: Platform={plat}, Account={acc}, Email={em}, UserID={uid}, Deleted={item.get('is_deleted')}")

    # 2. Test the specific query logic
    print("\n--- Testing Specific Query ---")
    if user_id:
        results = await db.async_list_integrations(user_id=user_id)
        print(f"Found {len(results)} items via async_list_integrations(user_id=...)")
        pprint(results)
    
    await db.close()

if __name__ == "__main__":
    target_user_id = "fa454f19-02b0-4065-8189-4b799ef7e3ea"
    asyncio.run(debug_integrations(user_id=target_user_id))
