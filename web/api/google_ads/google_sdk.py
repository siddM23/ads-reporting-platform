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

async def init_db():
    print("Initializing Google DB connections...")
    await metrics_db.connect()
    await integrations_db.connect()

async def close_db():
    print("Closing Google DB connections...")
    await metrics_db.close()
    await integrations_db.close()

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

def get_account_name(customer_id, refresh_token):
    """
    Fetches the descriptive name of a Google Ads account.
    """
    try:
        client = get_google_client(refresh_token, login_customer_id=customer_id)
        ga_service = client.get_service("GoogleAdsService")
        query = "SELECT customer.descriptive_name FROM customer LIMIT 1"
        
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = str(customer_id)
        search_request.query = query
        
        response = ga_service.search(request=search_request)
        for row in response:
            return row.customer.descriptive_name
    except Exception as e:
        print(f"Error fetching name for {customer_id}: {e}")
        return None
    return None

def discover_accounts(refresh_token, email=None):
    """
    Returns a list of accessible Client accounts for the given token.
    Skips Manager accounts to avoid "Metrics cannot be requested for manager" errors.
    """
    if email and email in _discovery_cache:
        print(f"GOOGLE DISCOVERY: Using cached IDs for {email}")
        return _discovery_cache[email]

    try:
        client = get_google_client(refresh_token)
        customer_service = client.get_service("CustomerService")
        
        print(f"GOOGLE DISCOVERY: Listing direct accessible customers...")
        accessible_customers = customer_service.list_accessible_customers()
        resource_names = accessible_customers.resource_names
        base_ids = [rn.split("/")[-1] for rn in resource_names]
        
        found_accounts = {} # ID -> Name
        
        for bid in base_ids:
            # Check if this base account is a manager or a client
            is_manager, name = get_account_info(bid, refresh_token)
            
            if is_manager:
                print(f"GOOGLE DISCOVERY: {bid} is a Manager. Finding sub-accounts...")
                sub_accounts = find_sub_accounts_sdk(bid, refresh_token)
                for sub in sub_accounts:
                    found_accounts[sub['id']] = sub['name']
            else:
                # Direct client account
                found_accounts[bid] = name or f"Google Account ({bid})"

        # Return list of {id, name}
        result = [{'id': k, 'name': v} for k, v in found_accounts.items()]
        
        if email:
            _discovery_cache[email] = result
        return result
    except Exception as e:
        print(f"GOOGLE DISCOVERY SDK ERROR: {e}")
        return []

def get_account_info(customer_id, refresh_token):
    """
    Returns (is_manager: bool, name: str) for a given account.
    """
    try:
        client = get_google_client(refresh_token, login_customer_id=customer_id)
        ga_service = client.get_service("GoogleAdsService")
        query = "SELECT customer.descriptive_name, customer.manager FROM customer LIMIT 1"
        
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = str(customer_id)
        search_request.query = query
        
        response = ga_service.search(request=search_request)
        for row in response:
            return row.customer.manager, row.customer.descriptive_name
    except Exception as e:
        print(f"Error fetching info for {customer_id}: {e}")
        return False, None
    return False, None

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
        
        client_accounts = []
        for row in response:
            client_client = row.customer_client
            # Only get actual client accounts, not sub-managers
            if not client_client.manager:
                cid = client_client.client_customer.split("/")[-1]
                name = client_client.descriptive_name or f"Google Account ({cid})"
                client_accounts.append({'id': cid, 'name': name})
                
        print(f"GOOGLE DISCOVERY: Found {len(client_accounts)} clients under manager {manager_id}")
        return client_accounts
    except Exception as e:
        # Some accounts might not be managers, ignore errors
        print(f"GOOGLE SUB-ACCOUNT DISCOVERY: {manager_id} skip or error: {e}")
        return []

def fetch_for_customer(customer_id, refresh_token, days=7, login_customer_id=None, start_date=None, end_date=None):
    """
    Fetches campaign-level insights for a single Google Ads Account using official SDK.
    """
    if not DEVELOPER_TOKEN:
        print(f"[{customer_id}] Error: GOOGLE_DEVELOPER_TOKEN not set in global.env")
        return []

    try:
        # 1. Time Range Calculation
        if start_date and end_date:
             # Use provided custom range
             print(f"[{customer_id}] Fetching Google insights (SDK) for custom range: {start_date} to {end_date}...")
        else:
             # Default to last 'days'
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
                "date_start": start_date,
                "date_stop": end_date,
                # Mimic Meta structure for frontend compatibility
                "website_purchase_roas": [{"value": str(roas)}],
                "action_values": [{"action_type": "conversions_value", "value": str(conv_value)}],
                "actions": [{"action_type": "conversions", "value": str(conversions)}]
            })
            
        print(f"[{customer_id}] Successfully fetched {len(formatted_data)} campaign rows via SDK.")
        return formatted_data

    except Exception as e:
        error_str = str(e)
        
        # Check for manager account errors
        is_manager_error = (
            "REQUESTED_METRICS_FOR_MANAGER" in error_str or 
            "Metrics cannot be requested for a manager account" in error_str or
            "METRICS_CANNOT_BE_REQUESTED_FOR_MANAGER" in error_str
        )

        if is_manager_error:
            print(f"[{customer_id}] GOOGLE ADS INFO: Account {customer_id} is a Manager Account. Fetching sub-accounts...")
            try:
                # Use the current customer_id as the manager context for children
                sub_accounts_list = find_sub_accounts_sdk(customer_id, refresh_token)
                all_sub_data = []
                for sub_acc in sub_accounts_list:
                    sub_id = sub_acc['id']
                    print(f"[{customer_id}] -> Fetching child sub-account {sub_id} ({sub_acc['name']})...")
                    sub_data = fetch_for_customer(
                        sub_id, 
                        refresh_token, 
                        days=days, 
                        login_customer_id=customer_id, # Very important: use parent as login context
                        start_date=start_date, 
                        end_date=end_date
                    )
                    if sub_data:
                        all_sub_data.extend(sub_data)
                return all_sub_data
            except Exception as sub_e:
                print(f"[{customer_id}] Error handling sub-accounts for manager: {sub_e}")
                return []
        
        elif "DEVELOPER_TOKEN_NOT_APPROVED" in error_str:
            print(f"[{customer_id}]GOOGLE ADS ERROR: Your Developer Token is only approved for TEST ACCOUNTS.")
        elif "PERMISSION_DENIED" in error_str:
            print(f"[{customer_id}]GOOGLE ADS ERROR: Permission Denied for account {customer_id}.")
            integrations_db.update_integration_status(platform="google", account_id=customer_id, needs_reauth=True, error_message="Permission Denied")
        elif "invalid_grant" in error_str or "unauthorized_client" in error_str:
            print(f"[{customer_id}] GOOGLE AUTH ERROR: Refresh token invalid.")
            integrations_db.update_integration_status(platform="google", account_id=customer_id, needs_reauth=True, error_message="Invalid Token")
        else:
            print(f"[{customer_id}]SDK Error fetching Google insights: {e}")
        return []

def write_to_dynamodb(data, days, integration_id=None):
    """
    Batch saves campaign analytics to the GoogleAdsInsights table.
    """
    if not data:
        return
    metrics_db.batch_write_campaign_metrics(data, days, integration_id=integration_id)

def fetch_and_store(days: int = 7, integration_ids: list = None):
    """
    Fetches data for connected Google accounts and stores in DynamoDB.
    """
    if integration_ids is not None:
        # STRICT ISOLATION: If an empty list of IDs is provided, do nothing.
        if not integration_ids:
            return []
            
        all_google = integrations_db.list_integrations(platform="google")
        integrations = [i for i in all_google if i.get('id') in integration_ids]
    else:
        # GLOBAL FALLBACK: Only if specifically requested via None
        integrations = integrations_db.list_integrations(platform="google")
    
    if not integrations:
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
                for acc_obj in customer_ids:
                    real_cid = acc_obj['id']
                    real_name = acc_obj['name']
                    integrations_db.save_integration(
                        platform="google",
                        account_id=real_cid,
                        account_name=real_name,
                        email=email,
                        access_token=token
                    )
            else:
                print(f"GOOGLE SYNC: No Google Ads accounts found associated with email {cid}. Stopping sync for this account.")
                customer_ids = []
        else:
            # Wrap standard CID in the expected structure for loop
            customer_ids = [{'id': cid, 'name': f"Google Account ({cid})"}]

        for target_obj in customer_ids:
            target_cid = target_obj['id']
            
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
                # We need the integration_id for this CID
                # If we are in the target_obj loop, we might need to find the specific integration record
                # However, for Google, sometimes we have one integration linked to many accounts.
                # But our save_integration creates a separate record for each REAL CID.
                # So we should look up the integration_id for target_cid.
                
                # Fetch integration to get its real ID
                # (Standard cid vs discovered cid)
                current_integration = None
                integration_list = integrations_db.list_integrations(platform="google")
                for item in integration_list:
                    if item.get('account_id') == target_cid:
                        current_integration = item
                        break
                
                write_to_dynamodb(account_data, days, integration_id=current_integration.get('id') if current_integration else None)
                all_results.extend(account_data)
                
                # Check if we need to update the account name (if it's generic)
                # We do NOT use account_data[0] name because it might be a sub-account of a manager
                try:
                    current_name = account.get('account_name', '')
                    if not current_name or f"({target_cid})" in current_name:
                        print(f"GOOGLE SYNC: Updating generic name for {target_cid}...")
                        real_name = get_account_name(target_cid, raw_token)
                        if real_name:
                             integrations_db.save_integration(
                                platform="google",
                                account_id=target_cid,
                                account_name=real_name,
                                email=email,
                                access_token=token
                             )
                except Exception as update_e:
                    print(f"GOOGLE SYNC warning: could not update account name: {update_e}")

            else:
                print(f"GOOGLE SYNC: No performance data found for CID {target_cid} in the last {days} days.")
            
    print(f"GOOGLE SYNC COMPLETE: Total {len(all_results)} campaigns synced for {days} days.")
    return all_results

def fetch_custom_range(start_date, end_date, integration_ids: list = None):
    """
    Fetches data for specific Google accounts for a custom date range.
    """
    if integration_ids:
        all_google = integrations_db.list_integrations(platform="google")
        integrations = [i for i in all_google if i.get('id') in integration_ids]
    else:
        integrations = integrations_db.list_integrations(platform="google")
    
    if not integrations:
        return []

    print(f"GOOGLE CUSTOM FETCH: Starting fetch for {len(integrations)} integrations (Range: {start_date} to {end_date})")
    
    all_results = []
    
    for account in integrations:
        email = account.get('email')
        token = account.get('access_token')
        cid = account.get('account_id')
        
        if not email or not token or not cid:
            continue
            
        raw_token = decrypt_token(token)
        
        # Discovery handling (simplified for custom fetch - assume CIDs are resolved or we skip complex discovery for speed)
        # However, we must handle email-based CIDs if they exist in DB
        customer_ids = [cid]
        if "@" in str(cid):
             # Try to discover on the fly or just skip to avoid blocking ui? 
             # Better to skip email-cids in custom fetch if they weren't resolved by sync
             continue
             
        for target_cid in customer_ids:
            if "@" in str(target_cid): continue

            # Fetch
            account_data = fetch_for_customer(
                target_cid, 
                raw_token, 
                days=0, 
                start_date=start_date, 
                end_date=end_date
            )
            
            if account_data:
                all_results.extend(account_data)

    return all_results

import concurrent.futures

def fetch_and_store_all(integration_ids: list = None):
    """
    Syncs data for all 3 dashboard time ranges: 7, 30, and 180 days.
    """
    print(f"Starting Google multi-range sync for {len(integration_ids) if integration_ids else 'all'} accounts...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        days_list = [7, 30, 180]
        future_to_days = {executor.submit(fetch_and_store, days, integration_ids): days for days in days_list}
        
        for future in concurrent.futures.as_completed(future_to_days):
            days = future_to_days[future]
            try:
                future.result()
                print(f"Google Sync for {days} days completed.")
            except Exception as e:
                print(f"Error fetching Google for range {days}: {e}")
                
    print("Full Google multi-range sync completed.")

async def async_get_cached_insights(days: int = 7, integration_ids: list = None):
    """
    Asynchronously returns data from DynamoDB without hitting Google API.
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
            row['platform'] = 'google'
    return data

if __name__ == "__main__":
    fetch_and_store_all()
