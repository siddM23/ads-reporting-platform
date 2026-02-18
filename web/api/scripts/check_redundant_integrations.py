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

async def check_redundant_integrations():
    print("--- 🔍 Checking for Redundant Integrations ---")
    db = DynamoDB(table_name="Integrations")
    await db.connect()
    
    response = await db.async_table.scan()
    items = response.get('Items', [])
    while 'LastEvaluatedKey' in response:
        response = await db.async_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))
        
    print(f"Total Integrations: {len(items)}")
    
    # Key: (platform, account_id)
    # Value: list of integration items
    account_map = defaultdict(list)
    for item in items:
        if not item.get('is_deleted'):
            key = (item.get('platform'), item.get('account_id'))
            account_map[key].append(item)
            
    redundant = {k: v for k, v in account_map.items() if len(v) > 1}
    print(f"Accounts with multiple ACTIVE integrations: {len(redundant)}")
    
    if redundant:
        for k, v in redundant.items():
            print(f"\nAccount {k}:")
            for i in v:
                print(f"  - ID: {i['id']} | Email: {i.get('email')} | Created: {i.get('created_at')}")
                
    await db.close()

if __name__ == "__main__":
    asyncio.run(check_redundant_integrations())
