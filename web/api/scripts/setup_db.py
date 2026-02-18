
import os
import sys

# Add path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

# Load env variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

def setup_database():
    print("Setting up DynamoDB Tables...")
    
    # 1. Integrations Table (modified for UUID pk, etc. already done)
    print("\nSetting up 'Integrations' table...")
    integrations_db = DynamoDB(table_name="Integrations")
    if integrations_db.create_table(pk='id', sk=None):
        print("Integrations table ready.")
        integrations_db.create_integrations_gsis()
    else:
        print("Failed to create Integrations table.")

    # 2. Meta Metrics Table
    print("\nSetting up 'Meta_Ad_Metrics' table...")
    meta_db = DynamoDB(table_name="Meta_Ad_Metrics")
    if meta_db.create_table(pk='campaign_id', sk='range_days', sk_type='N'):
        print("Meta Metrics table ready.")
        meta_db.create_range_days_gsi()
    else:
        print("Failed to create Meta Metrics table.")

    # 3. Google Metrics Table
    print("\nSetting up 'Google_Ad_Metrics' table...")
    google_db = DynamoDB(table_name="Google_Ad_Metrics")
    if google_db.create_table(pk='campaign_id', sk='range_days', sk_type='N'):
        print("Google Metrics table ready.")
        google_db.create_range_days_gsi()
    else:
        print("Failed to create Google Metrics table.")

    # 4. Users Table (UPDATED to UUID schema)
    print("\nSetting up 'Users' table...")
    users_db = DynamoDB(table_name="Users")
    # Change PK from 'email' to 'id' (UUID)
    if users_db.create_table(pk='id', sk=None):
        print("Users table ready.")
        users_db.create_users_gsis()
    else:
        print("Failed to create Users table.")

    print("\nDatabase setup complete!")

if __name__ == "__main__":
    setup_database()
