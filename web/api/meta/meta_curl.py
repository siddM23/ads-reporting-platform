import requests
import datetime
import json
import os
from dotenv import load_dotenv
from utils.security import decrypt_token


# Load env immediately to ensure DB has credentials
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

# Meta API Version
FB_VERSION = "v24.0"

from Database.database import DynamoDB

# Initialize Database connections
metrics_db = DynamoDB(table_name="MetaAdsInsights")
integrations_db = DynamoDB(table_name="Integrations")

async def init_db():
    print("Initializing Meta DB connections...")
    await metrics_db.connect()
    await integrations_db.connect()

async def close_db():
    print("Closing Meta DB connections...")
    await metrics_db.close()
    await integrations_db.close()

def fetch_for_account(account_id, token, days=7, start_date=None, end_date=None):
    """
    Fetches campaign-level insights for a single Meta Ad Account.
    """
    try:
        # 1. Clean up account_id to ensure it has 'act_' prefix
        clean_id = account_id.strip()
        if not clean_id.startswith('act_'):
            clean_id = f"act_{clean_id}"

        # 2. Time Range Calculation
        if start_date and end_date:
            time_range = {"since": start_date, "until": end_date}
            print(f"[{account_id}] Fetching Meta insights for custom range: {start_date} to {end_date}...")
        else:
            # Default to last 'days'
            s_date = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
            e_date = datetime.date.today().isoformat()
            time_range = {"since": s_date, "until": e_date}
            print(f"[{account_id}] Fetching Meta insights for last {days} days (from {s_date})...")

        # 3. Make the API request
        url = f"https://graph.facebook.com/{FB_VERSION}/{clean_id}/insights"
        params = {
            "level": "campaign",
            "fields": "campaign_id,campaign_name,spend,website_purchase_roas,action_values,actions,date_start,date_stop",
            "time_range": json.dumps(time_range),
            "access_token": token,
            "limit": 500  # Fetch more rows per page
        }

        all_data = []
        
        while True:
            r = requests.get(url, params=params)
            r.raise_for_status()
            
            res_json = r.json()
            data = res_json.get("data", [])
            all_data.extend(data)
            
            # Check for next page
            paging = res_json.get("paging", {})
            next_url = paging.get("next")
            
            if not next_url:
                break
                
            # Update url and remove params since next_url already contains them
            url = next_url
            params = {}

        print(f"[{account_id}] Successfully fetched {len(all_data)} total campaign rows across all pages.")
        return all_data

    except Exception as e:
        is_auth_error = False
        error_msg = str(e)
        
        # Check for 401/403 or specific Meta error codes
        if isinstance(e, requests.exceptions.HTTPError):
            try:
                error_data = e.response.json().get('error', {})
                code = error_data.get('code')
                subcode = error_data.get('error_subcode')
                message = error_data.get('message', '')
                
                # Meta Error 190: Access Token Expired
                # 401: Unauthorized
                if r.status_code == 401 or code == 190:
                    is_auth_error = True
                    error_msg = f"Meta Auth Error {code}: {message}"
                    print(f"[{account_id}] META AUTH INVALID: {message}")
                else:
                    print(f"[{account_id}] Meta API Error: {message}")
            except:
                print(f"[{account_id}] Meta API Error: {e.response.text}")

        if is_auth_error:
             integrations_db.update_integration_status(
                platform="meta",
                account_id=account_id.replace('act_', ''), # DB stores ID without prefix usually? Let's check. 
                # Actually DB stores what we get from /me/adaccounts. On entry.py line 497 we store acc['account_id'].
                # Meta usually provides raw numbers there. fetch_for_account receives 'act_' prefixed or raw.
                # We need to ensure we use the same ID format as stored in DB.
                # database.py save_integration calls str(account_id).
                # fetch_for_account takes account_id.
                # We should strip act_ just in case to match DB key.
                needs_reauth=True,
                error_message=error_msg
            )
        else:
            print(f"[{account_id}] Error fetching Meta insights: {e}")
        return []

def write_to_dynamodb(data, days, integration_id=None):
    """
    Batch saves campaign analytics to the MetaAdsInsights table.
    """
    if not data:
        return
    # Batch write for efficiency
    metrics_db.batch_write_campaign_metrics(data, days, integration_id=integration_id)

def _fetch_single_account(account, days):
    """Helper to fetch and store data for a single account."""
    account_id = account.get('account_id')
    token = account.get('access_token')
    integration_id = account.get('id')
    
    if not account_id or not token:
        return []
        
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
            access_token=token,
            account_name=acc_name
        )

    # Batch write to DynamoDB
    if account_data:
        write_to_dynamodb(account_data, days, integration_id=integration_id)
        
    return account_data

def fetch_and_store(days: int = 7, integration_ids: list = None):
    """
    Fetches data for connected Meta accounts and stores in DynamoDB.
    """
    if integration_ids:
        all_meta = integrations_db.list_integrations(platform="meta")
        integrations = [i for i in all_meta if i.get('id') in integration_ids]
    else:
        integrations = integrations_db.list_integrations(platform="meta")
    
    if not integrations:
        print("No Meta integrations found to sync.")
        return []

    print(f"Syncing {len(integrations)} Meta accounts for {days} days in parallel...")
    
    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_fetch_single_account, acc, days) for acc in integrations]
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                if res:
                    all_results.extend(res)
            except Exception as e:
                print(f"Error in parallel Meta fetch: {e}")
            
    print(f"✅ Synced {len(all_results)} campaigns for {days} days")
    return all_results

def _fetch_single_custom_range_account(account, start_date, end_date):
    """Helper for custom range fetch of a single account."""
    account_id = account.get('account_id')
    token = account.get('access_token')
    
    if not account_id or not token:
        return []
        
    # Fetch from Meta API
    account_data = fetch_for_account(
        account_id, 
        decrypt_token(token), 
        days=0, 
        start_date=start_date, 
        end_date=end_date
    )

    acc_name = account.get('account_name', f"Account {account_id}")

    for row in account_data:
        row['account_name'] = acc_name
        row['platform'] = 'meta'
        
    return account_data

def fetch_custom_range(start_date, end_date, integration_ids: list = None):
    """
    Fetches data for specific Meta accounts for a custom date range.
    """
    if integration_ids:
        all_meta = integrations_db.list_integrations(platform="meta")
        integrations = [i for i in all_meta if i.get('id') in integration_ids]
    else:
        integrations = integrations_db.list_integrations(platform="meta")
    
    if not integrations:
        return []

    print(f"Fetching Meta custom range: {start_date} to {end_date} for {len(integrations)} accounts in parallel...")
    
    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_fetch_single_custom_range_account, acc, start_date, end_date) for acc in integrations]
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                if res:
                    all_results.extend(res)
            except Exception as e:
                print(f"Error in parallel Meta custom fetch: {e}")
            
    return all_results

import concurrent.futures

def fetch_and_store_all(integration_ids: list = None):
    """
    Syncs data for all 3 dashboard time ranges: 7, 30, and 180 days.
    Uses threaded workers to fetch ranges concurrently.
    """
    print(f"🚀 Starting full multi-range sync for {len(integration_ids) if integration_ids else 'all'} accounts...")
    
    # Run fetches for 7, 30, and 180 days in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        days_list = [7, 30, 180]
        future_to_days = {executor.submit(fetch_and_store, days, integration_ids): days for days in days_list}
        
        for future in concurrent.futures.as_completed(future_to_days):
            days = future_to_days[future]
            try:
                future.result()
                print(f"✅ Sync for {days} days completed.")
            except Exception as e:
                print(f"❌ Error fetching for range {days}: {e}")
                
    print("✅ Full multi-range sync completed.")

async def async_get_cached_insights(days: int = 7, integration_ids: list = None):
    """
    Asynchronously returns data from DynamoDB without hitting Meta API.
    If integration_ids is provided, it returns only data for those accounts.
    """
    if integration_ids is not None:
        # Use the new granular index for isolation
        data = await metrics_db.async_read_metrics_by_integrations(integration_ids, days)
    else:
        # Legacy/Global fallback (Only if no scope is provided)
        data = await metrics_db.async_read_campaign_metrics(days)
        
    for row in data:
        if 'platform' not in row:
            row['platform'] = 'meta'
    return data

if __name__ == "__main__":
    fetch_and_store_all()
