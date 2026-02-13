import datetime
import json
import os
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient

from utils.security import decrypt_token

# Load env
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

# Google Ads API Version
GOOGLE_ADS_VERSION = "v18"

from Database.database import DynamoDB

# Initialize Database connections
metrics_db = DynamoDB(table_name="GoogleAdsInsights")
integrations_db = DynamoDB(table_name="Integrations")

DEVELOPER_TOKEN = os.getenv("GOOGLE_DEVELOPER_TOKEN", "")
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")

# Simple in-memory cache for discovery to prevent redundant calls in parallel threads
_discovery_cache = {}

def get_google_client(refresh_token, login_customer_id=None):
    """
    Creates a GoogleAdsClient from refresh token and env credentials.
    If login_customer_id is provided, it's used for manager account access.
    """
    credentials = {
        "developer_token": DEVELOPER_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "use_proto_plus": True
    }
    if login_customer_id:
        credentials["login_customer_id"] = str(login_customer_id)
    
    return GoogleAdsClient.load_from_dict(credentials)


def discover_accounts(refresh_token, email=None):
    """
    Returns a list of accessible customer IDs for the given token using Google Ads Client.
    """
    if email and email in _discovery_cache:
        print(f"GOOGLE DISCOVERY: Using cached IDs for {email}")
        return _discovery_cache[email]

    try:
        client = get_google_client(refresh_token)
        customer_service = client.get_service("CustomerService")
        
        print(f"GOOGLE DISCOVERY: Listing accessible customers using SDK...")
        accessible_customers = customer_service.list_accessible_customers()
        resource_names = accessible_customers.resource_names
        customer_ids = [rn.split("/")[-1] for rn in resource_names]
        
        print(f"GOOGLE DISCOVERY: Found {len(customer_ids)} base accounts: {customer_ids}")
        
        # Now, for each base account, check if it's a manager and find its sub-accounts
        all_discovered_ids = set(customer_ids)
        for base_id in customer_ids:
            sub_ids = find_sub_accounts_sdk(base_id, refresh_token)
            all_discovered_ids.update(sub_ids)
        
        result = list(all_discovered_ids)
        if email:
            _discovery_cache[email] = result
        return result
    except Exception as e:
        print(f"GOOGLE DISCOVERY SDK ERROR: {e}")
        return []

def find_sub_accounts_sdk(manager_id, refresh_token):
    """
    Given a manager ID, finds all sub-accounts (clients) under it using SDK.
    """
    try:
        print(f"GOOGLE DISCOVERY: Checking if {manager_id} has sub-accounts via SDK...")
        # For manager queries, we must set "login-customer-id"
        client = get_google_client(refresh_token, login_customer_id=manager_id)
        ga_service = client.get_service("GoogleAdsService")
        
        # Query for all client accounts under this manager
        query = "SELECT customer_client.client_customer, customer_client.descriptive_name, customer_client.manager FROM customer_client WHERE customer_client.level <= 1"
        
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = str(manager_id)
        search_request.query = query
        
        response = ga_service.search(request=search_request)
        
        client_ids = []
        for row in response:
            client_client = row.customer_client
            # Only get actual client accounts, not sub-managers
            if not client_client.manager:
                cid = client_client.client_customer.split("/")[-1]
                client_ids.append(cid)
                
        print(f"GOOGLE DISCOVERY: Found {len(client_ids)} clients under manager {manager_id}")
        return client_ids
    except Exception as e:
        # Some accounts might not be managers, ignore errors
        print(f"GOOGLE SUB-ACCOUNT DISCOVERY: {manager_id} skip or error: {e}")
        return []

def fetch_for_customer(customer_id, refresh_token, days, login_customer_id=None):
    """
    Fetches campaign-level insights for a single Google Ads Account using official SDK.
    """
    if not DEVELOPER_TOKEN:
        print(f"[{customer_id}] Error: GOOGLE_DEVELOPER_TOKEN not set in global.env")
        return []

    try:
        # 1. Time Range Calculation
        start_date = (datetime.date.today() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"[{customer_id}] Fetching Google insights (SDK) for last {days} days ({start_date} to {end_date})...")

        # 2. Initialize Client
        # If we have a login_customer_id (manager ID), use it; otherwise fallback to customer_id itself
        client = get_google_client(refresh_token, login_customer_id=login_customer_id or customer_id)
        ga_service = client.get_service("GoogleAdsService")

        # 3. GAQL Query
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                metrics.cost_micros,
                metrics.conversions_value,
                metrics.conversions,
                customer.descriptive_name
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """

        # 4. Request
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = str(customer_id)
        search_request.query = query

        response = ga_service.search(request=search_request)
        
        # 5. Transform
        formatted_data = []
        for row in response:
            campaign = row.campaign
            metrics = row.metrics
            customer = row.customer
            
            # Google cost is in micros (1/1,000,000)
            spend = float(metrics.cost_micros) / 1_000_000
            conv_value = float(metrics.conversions_value)
            conversions = float(metrics.conversions)
            
            roas = conv_value / spend if spend > 0 else 0
            
            formatted_data.append({
                "campaign_id": str(campaign.id),
                "campaign_name": campaign.name,
                "spend": str(spend),
                "account_name": customer.descriptive_name or f"Account {customer_id}",
                "platform": "google",
                # Mimic Meta structure for frontend compatibility
                "website_purchase_roas": [{"value": str(roas)}],
                "action_values": [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": str(conv_value)}],
                "actions": [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": str(conversions)}]
            })
            
        print(f"[{customer_id}] Successfully fetched {len(formatted_data)} campaign rows via SDK.")
        return formatted_data

    except Exception as e:
        print(f"[{customer_id}] SDK Error fetching Google insights: {e}")
        return []

def write_to_dynamodb(data, days):
    """
    Batch saves campaign analytics to the GoogleAdsInsights table.
    """
    if not data:
        return
    metrics_db.create_table(pk='campaign_id', sk='range_days', sk_type='N')
    metrics_db.batch_write_campaign_metrics(data, days)

def fetch_and_store(days: int = 7):
    """
    Fetches data for all connected Google accounts and stores in DynamoDB.
    """
    integrations = integrations_db.list_integrations(platform="google")
    
    if not integrations:
        print("No Google integrations found.")
        return []

    print(f"GOOGLE SYNC: Starting fetch for {len(integrations)} integrations (Range: {days} days)")
    
    all_results = []
    
    for account in integrations:
        email = account.get('email')
        token = account.get('access_token')
        cid = account.get('account_id')
        
        if not email or not token or not cid:
            print(f"GOOGLE SYNC: Skipping account due to missing data: {email} (CID: {cid})")
            continue
            
        # The SDK handles its own token refresh if we give it the refresh token.
        # decrypt_token(token) should be the refresh token (starts with 1//).
        raw_token = decrypt_token(token)
        
        # 2. Handle Case where CID is an email (needs discovery)
        customer_ids = []
        if "@" in str(cid):
            print(f"GOOGLE SYNC: CID is an email ({cid}), attempting discovery via SDK...")
            customer_ids = discover_accounts(raw_token, email=email)
            
            if customer_ids:
                print(f"GOOGLE SYNC: Found {len(customer_ids)} IDs for {cid}. Updating integration records...")
                for real_cid in customer_ids:
                    integrations_db.save_integration(
                        platform="google",
                        account_id=real_cid,
                        account_name=f"Google Account ({real_cid})",
                        email=email,
                        access_token=token
                    )
            else:
                print(f"GOOGLE SYNC: No Google Ads accounts found associated with email {cid}. Stopping sync for this account.")
                customer_ids = []
        else:
            customer_ids = [cid]

        for target_cid in customer_ids:
            if "@" in str(target_cid):
                print(f"GOOGLE SYNC: Skipping API call for non-numeric CID: {target_cid}")
                continue

            print(f"GOOGLE SYNC: Fetching metrics for numeric CID {target_cid} ({email})...")
            # For target_cid, we pass target_cid as login_customer_id if it's a direct account.
            # If it's a sub-account of a manager, the SDK might need the manager CID, 
            # but usually the account CID itself works if we have permissions.
            account_data = fetch_for_customer(target_cid, raw_token, days)
            
            if account_data:
                print(f"GOOGLE SYNC: Found {len(account_data)} campaigns for CID {target_cid}. Writing to DB...")
                write_to_dynamodb(account_data, days)
                all_results.extend(account_data)
            else:
                print(f"GOOGLE SYNC: No performance data found for CID {target_cid} in the last {days} days.")
            
    print(f"✅ GOOGLE SYNC COMPLETE: Total {len(all_results)} campaigns synced for {days} days.")
    return all_results

import concurrent.futures

def fetch_and_store_all():
    """
    Syncs data for all 3 dashboard time ranges: 7, 30, and 180 days.
    """
    print("🚀 Starting Google multi-range sync...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        days_list = [7, 30, 180]
        future_to_days = {executor.submit(fetch_and_store, days): days for days in days_list}
        
        for future in concurrent.futures.as_completed(future_to_days):
            days = future_to_days[future]
            try:
                future.result()
                print(f"✅ Google Sync for {days} days completed.")
            except Exception as e:
                print(f"❌ Error fetching Google for range {days}: {e}")
                
    print("✅ Full Google multi-range sync completed.")

def get_cached_insights(days: int = 7):
    """
    Returns data from DynamoDB.
    Ensures 'platform' field is present for frontend filtering.
    """
    data = metrics_db.read_campaign_metrics(days)
    for row in data:
        if 'platform' not in row:
            row['platform'] = 'google'
    return data

if __name__ == "__main__":
    fetch_and_store_all()
