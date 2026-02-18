
import asyncio
import os
import sys
import boto3
from boto3.dynamodb.conditions import Key, Attr
from pprint import pprint

# Ensure path 
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

async def test_get_user(email):
    print(f"Testing async_get_user for email: {email}")
    db = DynamoDB(table_name="Users")
    await db.connect()
    
    try:
        # 1. Direct call to async_get_user
        print("\nCalling async_get_user...")
        user = await db.async_get_user(email)
        print(f"Result for {email}: {user}")
        
        # 2. Check async_query directly if needed
        if not user:
            print("\nTrying direct async_query...")
            resp = await db.async_query(
                IndexName='EmailIndex',
                KeyConditionExpression=Key('email').eq(email)
            )
            print(f"Direct query response: {resp.get('Items')}")
            
    except Exception as e:
        print(f"Error during test: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    test_email = "test_user_logic@example.com" # Should exist
    asyncio.run(test_get_user(test_email))
