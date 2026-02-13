import requests
import datetime
import json
import os
from dotenv import load_dotenv

from utils.security import decrypt_token

# Load env
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

# Google Ads API Version
GOOGLE_ADS_VERSION = "v17"

from Database.database import DynamoDB

# Initialize Database connections
metrics_db = DynamoDB(table_name="GoogleAdsInsights")
integrations_db = DynamoDB(table_name="Integrations")

DEVELOPER_TOKEN = os.getenv("GOOGLE_DEVELOPER_TOKEN", "")
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")

def get_access_token(stored_token):
    """
    If stored_token starts with '1//', it's likely a refresh token. 
    Google access tokens usually start with 'ya29.'.
    Returns a valid access token.
    """
    if stored_token.startswith("ya29."):
        return stored_token
        
    print(f"GOOGLE SYNC: Refreshing token...")
    url = "https://oauth2.googleapis.com/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": stored_token,
        "grant_type": "refresh_token"
    }
    r = requests.post(url, data=payload)
    if r.ok:
        new_token = r.json().get("access_token")
        print(f"GOOGLE SYNC: Token refreshed successfully.")
        return new_token
    else:
        print(f"GOOGLE SYNC: Token refresh FAILED ({r.status_code}): {r.text}")
        return stored_token # Fallback

def discover_accounts(access_token):
    """
    Returns a list of accessible customer IDs for the given token.
    """
    try:
        url = f"https://googleads.googleapis.com/{GOOGLE_ADS_VERSION}/customers:listAccessibleCustomers"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "developer-token": DEVELOPER_TOKEN
        }
        r = requests.get(url, headers=headers)
        if r.ok:
            print(f"GOOGLE DISCOVERY: Raw response: {r.text}")
            resource_names = r.json().get("resourceNames", [])
            customer_ids = [rn.split("/")[-1] for rn in resource_names]
            print(f"GOOGLE DISCOVERY: Found {len(customer_ids)} accessible customers: {customer_ids}")
            return customer_ids
        else:
            print(f"GOOGLE DISCOVERY: API failed ({r.status_code}): {r.text}")
            return []
    except Exception as e:
        print(f"GOOGLE DISCOVERY: Exception: {e}")
        return []

def fetch_for_customer(customer_id, token, days, login_customer_id=None):
    """
    Fetches campaign-level insights for a single Google Ads Account via REST API.
    """
    if not DEVELOPER_TOKEN:
        print(f"[{customer_id}] Error: GOOGLE_DEVELOPER_TOKEN not set in global.env")
        return []

    try:
        # 1. Time Range Calculation
        start_date = (datetime.date.today() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')
        end_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"[{customer_id}] Fetching Google insights for last {days} days ({start_date} to {end_date})...")

        # 2. GAQL Query
        # We fetch cost, conversions, and conversion value
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

        # 3. Request
        url = f"https://googleads.googleapis.com/{GOOGLE_ADS_VERSION}/customers/{customer_id}/googleAds:search"
        headers = {
            "Authorization": f"Bearer {token}",
            "developer-token": DEVELOPER_TOKEN,
            "Content-Type": "application/json"
        }
        if login_customer_id:
            headers["login-customer-id"] = str(login_customer_id)

        payload = {"query": query}
        
        r = requests.post(url, headers=headers, json=payload)
        
        if not r.ok:
            print(f"[{customer_id}] GOOGLE API ERROR {r.status_code} (URL: {url}): {r.text}")
            r.raise_for_status()
        
        full_response = r.json()
        rows = full_response.get("results", [])
        
        if not rows:
            print(f"[{customer_id}] Google returned 0 total campaign rows for date range {start_date} to {end_date}.")
            # Log the request ID for support if needed
            print(f"[{customer_id}] Request-ID: {r.headers.get('request-id')}")
        
        # 4. Transform to a format similar to Meta's for the frontend
        formatted_data = []
        for row in rows:
            campaign = row.get("campaign", {})
            metrics = row.get("metrics", {})
            customer = row.get("customer", {})
            
            # Google cost is in micros (1/1,000,000)
            spend = float(metrics.get("costMicros", 0)) / 1_000_000
            conv_value = float(metrics.get("conversionsValue", 0))
            conversions = float(metrics.get("conversions", 0))
            
            roas = conv_value / spend if spend > 0 else 0
            
            formatted_data.append({
                "campaign_id": campaign.get("id"),
                "campaign_name": campaign.get("name"),
                "spend": str(spend),
                "account_name": customer.get("descriptiveName", f"Account {customer_id}"),
                "platform": "google",
                # Mimic Meta structure for frontend compatibility
                "website_purchase_roas": [{"value": str(roas)}],
                "action_values": [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": str(conv_value)}],
                "actions": [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": str(conversions)}]
            })
            
        print(f"[{customer_id}] Successfully fetched {len(formatted_data)} campaign rows.")
        return formatted_data

    except Exception as e:
        if hasattr(e, 'response') and e.response is not None:
            print(f"[{customer_id}] Google API Error: {e.response.text}")
        else:
            print(f"[{customer_id}] Error fetching Google insights: {e}")
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
            
        access_token = get_access_token(decrypt_token(token))
        
        # 2. Handle Case where CID is an email (needs discovery)
        customer_ids = []
        if "@" in str(cid):
            print(f"GOOGLE SYNC: CID is an email ({cid}), attempting discovery...")
            customer_ids = discover_accounts(access_token)
            
            if customer_ids:
                print(f"GOOGLE SYNC: Found {len(customer_ids)} IDs for {cid}. Updating integration records...")
                # Retroactively update/split integration records to use numeric IDs
                for real_cid in customer_ids:
                    integrations_db.save_integration(
                        platform="google",
                        account_id=real_cid,
                        account_name=f"Google Account ({real_cid})",
                        email=email,
                        access_token=token # Keep existing encrypted token
                    )
                # Note: We should ideally delete the 'email' record, but let's keep it safe for now.
            else:
                print(f"GOOGLE SYNC: No numeric IDs found for {cid}. API calls will likely fail.")
                customer_ids = [cid] # Fallback to original, likely will 404
        else:
            customer_ids = [cid]

        for target_cid in customer_ids:
            if "@" in str(target_cid):
                print(f"GOOGLE SYNC: Skipping API call for non-numeric CID: {target_cid}")
                continue

            print(f"GOOGLE SYNC: Fetching metrics for numeric CID {target_cid} ({email})...")
            account_data = fetch_for_customer(target_cid, access_token, days)
            
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
