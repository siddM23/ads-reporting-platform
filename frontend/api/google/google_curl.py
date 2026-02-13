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
            AND metrics.cost_micros > 0
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
            print(f"[{customer_id}] GOOGLE API ERROR {r.status_code}: {r.text}")
            r.raise_for_status()
        
        rows = r.json().get("results", [])
        
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

    print(f"Syncing {len(integrations)} Google accounts for {days} days...")
    
    all_results = []
    
    for account in integrations:
        email = account.get('email')
        token = account.get('access_token')
        
        if not email or not token:
            continue
            
        # For Google, we currently store email as account_id in Integrations.
        # We might need to handle the case where we need to fetch multiple account IDs for one email.
        # For now, let's assume one account or use a discovery step.
        
        # 1. Exchange refresh token for access token
        raw_token = decrypt_token(token)
        access_token = get_access_token(raw_token)
        
        # 2. Fetch Customer ID
        customer_ids = []
        if "@" in account.get('account_id', ''):
            # It's an email, let's try to discover customer IDs
            try:
                list_url = f"https://googleads.googleapis.com/{GOOGLE_ADS_VERSION}/customers:listAccessibleCustomers"
                list_r = requests.get(list_url, headers={"Authorization": f"Bearer {access_token}", "developer-token": DEVELOPER_TOKEN})
                if list_r.ok:
                    resource_names = list_r.json().get("resourceNames", [])
                    customer_ids = [rn.split("/")[-1] for rn in resource_names]
                    print(f"GOOGLE SYNC: Discovered {len(customer_ids)} customer IDs for {email}: {customer_ids}")
                else:
                    print(f"GOOGLE SYNC: Discovery API failed for {email} ({list_r.status_code}): {list_r.text}")
            except Exception as e:
                print(f"GOOGLE SYNC: Discovery failed for {email}: {e}")
        else:
            customer_ids = [account.get('account_id')]

        for cid in customer_ids:
            # Fetch from Google Ads API
            account_data = fetch_for_customer(cid, access_token, days)
            
            # Patch the integration record if account_name is missing
            if not account.get('account_name') and account_data:
                integrations_db.save_integration(
                    platform='google',
                    account_id=account.get('account_id'),
                    email=email,
                    access_token=token,
                    account_name=account_data[0].get('account_name', f"Google Account {account.get('account_id')}")
                )

            # Batch write to DynamoDB
            if account_data:
                write_to_dynamodb(account_data, days)
                all_results.extend(account_data)
            
    print(f"✅ Synced {len(all_results)} Google campaigns for {days} days")
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
