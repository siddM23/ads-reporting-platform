import os
import sys

# Add parent directory to path to import Database.database
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from Database.database import DynamoDB
from dotenv import load_dotenv

# Load environment variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

def setup_database():
    print("🚀 Starting Database Setup...")
    
    # 1. Integrations Table
    print("\nSetting up 'Integrations' table...")
    integrations_db = DynamoDB(table_name="Integrations")
    if integrations_db.create_table(pk='platform', sk='account_id', sk_type='S'):
        print("✅ Integrations table ready.")
    else:
        print("❌ Failed to create Integrations table.")

    # 2. Meta Ads Insights Table
    print("\nSetting up 'MetaAdsInsights' table...")
    meta_metrics_db = DynamoDB(table_name="MetaAdsInsights")
    if meta_metrics_db.create_table(pk='campaign_id', sk='range_days', sk_type='N'):
        print("✅ MetaAdsInsights table ready.")
        meta_metrics_db.create_range_days_gsi()
    else:
        print("❌ Failed to create MetaAdsInsights table.")

    # 3. Google Ads Insights Table
    print("\nSetting up 'GoogleAdsInsights' table...")
    google_metrics_db = DynamoDB(table_name="GoogleAdsInsights")
    if google_metrics_db.create_table(pk='campaign_id', sk='range_days', sk_type='N'):
        print("✅ GoogleAdsInsights table ready.")
        google_metrics_db.create_range_days_gsi()
    else:
        print("❌ Failed to create GoogleAdsInsights table.")

    # 4. Users Table
    print("\nSetting up 'Users' table...")
    users_db = DynamoDB(table_name="Users")
    if users_db.create_table(pk='email', sk='created_at', sk_type='S'):
        print("✅ Users table ready.")
    else:
        print("❌ Failed to create Users table.")

    print("\n🎉 Database setup complete!")

if __name__ == "__main__":
    setup_database()
