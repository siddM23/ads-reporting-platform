import requests
import datetime
import json
import os
from dotenv import load_dotenv
try:
    from utils.security import decrypt_token
except ImportError:
    # Handle path when run directly
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from utils.security import decrypt_token


# Load env immediately to ensure DB has credentials
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

# Meta API Version
FB_VERSION = "v24.0"

# Import DynamoDB
try:
    from Database.database import DynamoDB
except ImportError:
    # Vercel might need absolute-ish import depending on execution
    try:
        from api.Database.database import DynamoDB
    except ImportError:
        from frontend.api.Database.database import DynamoDB

# Initialize Database connections
metrics_db = DynamoDB(table_name="MetaAdsInsights")
integrations_db = DynamoDB(table_name="Integrations")

def fetch_for_account(account_id, token, days):
    """
    Fetches campaign-level insights for a single Meta Ad Account.
    """
    try:
        # 1. Clean up account_id to ensure it has 'act_' prefix
        clean_id = account_id.strip()
        if not clean_id.startswith('act_'):
            clean_id = f"act_{clean_id}"

        # 2. Time Range Calculation
        # Meta expects JSON object { 'since': 'YYYY-MM-DD', 'until': 'YYYY-MM-DD' }
        start_date = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
        end_date = datetime.date.today().isoformat()
        time_range = {"since": start_date, "until": end_date}

        print(f"[{account_id}] Fetching Meta insights for last {days} days (from {start_date})...")

        # 3. Make the API request
        url = f"https://graph.facebook.com/{FB_VERSION}/{clean_id}/insights"
        params = {
            "level": "campaign",
            "fields": "campaign_id,campaign_name,spend,website_purchase_roas,action_values,actions",
            "time_range": json.dumps(time_range),
            "access_token": token,
            "limit": 500  # Fetch more rows per page
        }

        r = requests.get(url, params=params)
        r.raise_for_status()
        
        data = r.json().get("data", [])
        print(f"[{account_id}] Successfully fetched {len(data)} campaign rows.")
        return data

    except Exception as e:
        # Print detailed error if it's a request error
        if isinstance(e, requests.exceptions.HTTPError):
            print(f"[{account_id}] Meta API Error: {e.response.text}")
        else:
            print(f"[{account_id}] Error fetching Meta insights: {e}")
        return []

def write_to_dynamodb(data, days):
    """
    Batch saves campaign analytics to the MetaAdsInsights table.
    """
    if not data:
        return
    # Ensure the metrics table exists before writing
    metrics_db.create_table(pk='campaign_id', sk='range_days', sk_type='N')
    # Use batch write for efficiency
    metrics_db.batch_write_campaign_metrics(data, days)

def fetch_and_store(days: int = 7):
    """
    Fetches data for all connected Meta accounts and stores in DynamoDB.
    """
    integrations = integrations_db.list_integrations(platform="meta")
    
    if not integrations:
        print("No Meta integrations found.")
        return []

    print(f"Syncing {len(integrations)} Meta accounts for {days} days...")
    
    all_results = []
    
    for account in integrations:
        account_id = account.get('account_id')
        token = account.get('access_token')
        
        if not account_id or not token:
            continue
            
        # Fetch from Meta API
        account_data = fetch_for_account(account_id, decrypt_token(token), days)

        
        # Get account name
        try:
            clean_id = account_id.strip()
            if not clean_id.startswith('act_'):
                clean_id = f"act_{clean_id}"
            name_r = requests.get(f"https://graph.facebook.com/{FB_VERSION}/{clean_id}", 
                                  params={"access_token": decrypt_token(token), "fields": "name"})

            acc_name = name_r.json().get("name", f"Account {account_id}")
        except:
            acc_name = f"Account {account_id}"

        # Add account name and platform to each row
        for row in account_data:
            row['account_name'] = acc_name
            row['platform'] = 'meta'
        
        # Patch the integration record if account_name is missing
        if not account.get('account_name'):
            integrations_db.save_integration(
                platform='meta',
                account_id=account_id,
                email=account.get('email'),
                access_token=token, # Already encrypted in the account object
                account_name=acc_name
            )

        # Batch write to DynamoDB
        if account_data:
            write_to_dynamodb(account_data, days)
            all_results.extend(account_data)
            
    print(f"✅ Synced {len(all_results)} campaigns for {days} days")
    return all_results

import concurrent.futures

def fetch_and_store_all():
    """
    Syncs data for all 3 dashboard time ranges: 7, 30, and 180 days.
    Uses threaded workers to fetch ranges concurrently.
    """
    print("🚀 Starting full multi-range sync...")
    
    # Run fetches for 7, 30, and 180 days in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        days_list = [7, 30, 180]
        future_to_days = {executor.submit(fetch_and_store, days): days for days in days_list}
        
        for future in concurrent.futures.as_completed(future_to_days):
            days = future_to_days[future]
            try:
                future.result()
                print(f"✅ Sync for {days} days completed.")
            except Exception as e:
                print(f"❌ Error fetching for range {days}: {e}")
                
    print("✅ Full multi-range sync completed.")

def get_cached_insights(days: int = 7):
    """
    Returns data from DynamoDB without hitting Meta API.
    Ensures 'platform' field is present for frontend filtering.
    """
    data = metrics_db.read_campaign_metrics(days)
    for row in data:
        if 'platform' not in row:
            row['platform'] = 'meta'
    return data

if __name__ == "__main__":
    fetch_and_store_all()
