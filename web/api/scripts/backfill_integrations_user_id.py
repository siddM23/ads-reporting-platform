
import os
import sys
import asyncio
from typing import Dict, Any

# Ensure path to database module
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

async def backfill_integrations_user_id():
    print("Backfilling user_id into Integrations table...")
    
    users_db = DynamoDB(table_name="Users")
    integrations_db = DynamoDB(table_name="Integrations")
    
    await users_db.connect()
    await integrations_db.connect()
    
    try:
        # 1. Fetch all users
        # We can scan the table since async_scan isn't directly exposed but we can use async_table.scan
        if not users_db.async_table:
             print("Error: Async table resource not initialized.")
             return

        print("Scanning Users...")
        response = await users_db.async_table.scan()
        users = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = await users_db.async_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            users.extend(response.get('Items', []))
            
        print(f"Found {len(users)} users.")
        
        # Build map: email -> user_id
        user_map = {u['email']: u['id'] for u in users if 'email' in u and 'id' in u}
        
        # 2. Fetch all integrations (or specific ones)
        print("Scanning Integrations...")
        response = await integrations_db.async_table.scan()
        integrations = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
             response = await integrations_db.async_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
             integrations.extend(response.get('Items', []))
             
        print(f"Found {len(integrations)} integrations.")
        
        updated_count = 0
        for integration in integrations:
            email = integration.get('email')
            current_user_id = integration.get('user_id')
            
            if email and email in user_map:
                target_user_id = user_map[email]
                
                # Update if missing or different
                if current_user_id != target_user_id:
                    print(f"Updating integration {integration['id']} for {email} with user_id: {target_user_id}")
                    
                    # Update item
                    await integrations_db.async_table.update_item(
                        Key={'id': integration['id']},
                        UpdateExpression="SET user_id = :uid",
                        ExpressionAttributeValues={":uid": target_user_id}
                    )
                    updated_count += 1
            else:
                if email:
                    print(f"Warning: Integration {integration['id']} has email {email} but no matching user found.")
                else:
                    print(f"Warning: Integration {integration['id']} has no email.")

        print(f"Backfill complete. Updated {updated_count} integrations.")
        
    except Exception as e:
        print(f"Error executing backfill: {e}")
    finally:
        await users_db.close()
        await integrations_db.close()

if __name__ == "__main__":
    asyncio.run(backfill_integrations_user_id())
