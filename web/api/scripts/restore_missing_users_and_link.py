
import os
import sys
import asyncio
import uuid
import datetime

# Ensure path to database module
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

async def restore_missing_users_and_link():
    print("Scanning Integrations for missing users...")
    
    users_db = DynamoDB(table_name="Users")
    integrations_db = DynamoDB(table_name="Integrations")
    
    await users_db.connect()
    await integrations_db.connect()
    
    try:
        # 1. Get all integrations
        resp = await integrations_db.async_table.scan()
        integrations = resp.get('Items', [])
        while 'LastEvaluatedKey' in resp:
            resp = await integrations_db.async_table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
            integrations.extend(resp.get('Items', []))
            
        print(f"Found {len(integrations)} integrations.")
        
        # 2. Extract unique emails
        integration_emails = set(i.get('email') for i in integrations if i.get('email'))
        print(f"Found {len(integration_emails)} unique emails in integrations: {integration_emails}")
        
        # 3. Check which users exist
        users_map = {} # email -> user_id
        
        for email in integration_emails:
            user = await users_db.async_get_user(email)
            if user:
                users_map[email] = user['id']
                print(f"  User found: {email} -> {user['id']}")
            else:
                print(f"  User MISSING: {email}")
                # Create the user!
                new_id = str(uuid.uuid4())
                timestamp = datetime.datetime.utcnow().isoformat()
                
                # We'll set a placeholder password or empty. 
                # Since we don't know the password, they might need to reset it or use a known hash.
                # For now, we'll use a placeholder 'RESET_REQUIRED' or similar if acceptable, 
                # but to be safe lets just replicate the structure.
                
                new_user = {
                    'id': new_id,
                    'email': email,
                    'created_at': timestamp,
                    'password_hash': 'RESET_REQUIRED_MIGRATION',
                    # Add default preferences if needed
                    'preferences': {
                        'selected_label': 'Default'
                    }
                }
                
                await users_db.async_table.put_item(Item=new_user)
                users_map[email] = new_id
                print(f"  Created NEW User: {email} -> {new_id}")
                
        # 4. Update Integrations with the correct user_id
        updated_count = 0
        for integration in integrations:
            email = integration.get('email')
            if not email: continue
            
            target_user_id = users_map.get(email)
            
            if target_user_id and integration.get('user_id') != target_user_id:
                print(f"Linking integration {integration['id']} ({email}) to user {target_user_id}")
                await integrations_db.async_table.update_item(
                    Key={'id': integration['id']},
                    UpdateExpression="SET user_id = :uid",
                    ExpressionAttributeValues={":uid": target_user_id}
                )
                updated_count += 1
                
        print(f"Finished. Linked {updated_count} integrations to users.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await users_db.close()
        await integrations_db.close()

if __name__ == "__main__":
    asyncio.run(restore_missing_users_and_link())
