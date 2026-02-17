import boto3
import os
from dotenv import load_dotenv

# Load env for AWS credentials
ENV_PATH = os.path.join(os.path.dirname(__file__), 'global.env')
if not os.path.exists(ENV_PATH):
    # Fallback if run from a different directory
    ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'global.env')

load_dotenv(ENV_PATH, override=True)

def reset_sync_limit():
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    # 1. Reset the actual tracker used by the application
    try:
        table_name = "SyncTracking"
        table = dynamodb.Table(table_name)
        table.delete_item(Key={'tracker_id': 'global'})
        print(f"✅ Sync limit reset in table '{table_name}'.")
    except Exception as e:
        print(f"❌ Could not reset '{table_name}': {e}")

    # 2. Reset the 'app_status' table mentioned by the user (if it exists)
    try:
        user_table_name = 'app_status'
        user_table = dynamodb.Table(user_table_name)
        user_table.delete_item(Key={'id': 'global_sync'})
        print(f"✅ Sync limit reset in table '{user_table_name}'.")
    except Exception as e:
        # Silently fail if table doesn't exist, as it's not the primary one
        pass

if __name__ == "__main__":
    reset_sync_limit()
