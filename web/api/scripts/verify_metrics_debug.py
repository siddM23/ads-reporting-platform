
import os
import sys
import asyncio
import datetime
import requests
import json
from dotenv import load_dotenv

# Path setup
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Load Env
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
load_dotenv(ENV_PATH, override=True)

from Database.database import DynamoDB
from utils.security import decrypt_token

async def debug_metrics():
    print("Initializing DB...")
    db = DynamoDB(table_name="Integrations")
    await db.connect()
    
    email = "socialcubehq@gmail.com"
    print(f"Fetching integrations for {email}...")
    integrations = await db.async_list_integrations(email=email)
    print(f"Found {len(integrations)} integrations.")
    
    target_acc = None
    target_name = "Holiday Traditions" 
    
    for i in integrations:
        name = i.get('account_name') or i.get('name') or "Unknown"
        acc_id = i.get('account_id')
        print(f" - {name} ({acc_id})")
        if name and target_name.lower() in name.lower():
            target_acc = i
            break
            
    if not target_acc:
        print(f"Account containing '{target_name}' not found.")
        await db.close()
        return

    print(f"Found Account: {target_acc.get('account_name')} ({target_acc.get('account_id')})")
    
    # Decrypt Token
    encrypted = target_acc.get('access_token')
    access_token = decrypt_token(encrypted)
    account_id = target_acc.get('account_id')
    
    # Define Dates
    # Current: Feb 11 - Feb 18 (8 days inclusive as per user dashboard?)
    current_start = "2026-02-11"
    current_end = "2026-02-18"
    
    # Previous: 7 days prior logic from entry.py (Duration = 7 days)
    # prev_end = current_start - 1 = Feb 10
    # prev_start = prev_end - 7 days = Feb 3
    prev_start = "2026-02-03"
    prev_end = "2026-02-10"
    
    date_ranges = [
        ("Current", current_start, current_end),
        ("Previous", prev_start, prev_end)
    ]
    
    campaign_name_fragment = "Cube- Advantage+ - Sales - 2025-1DC1DV"
    
    # Store all data: { "campaign_id": { "Current": metrics, "Previous": metrics } }
    all_campaigns_data = {}

    for label, start, end in date_ranges:
        print(f"\n--- Fetching {label} ({start} to {end}) ---")
        url = f"https://graph.facebook.com/v24.0/act_{account_id}/insights"
        params = {
            "access_token": access_token,
            "level": "campaign",
            "fields": "campaign_name,campaign_id,spend,actions,action_values,impressions,clicks",
            "time_range": json.dumps({"since": start, "until": end}),
            "limit": 500
        }
        
        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            print(f"API Error: {resp.text}")
            continue
            
        data = resp.json().get('data', [])
        print(f"DEBUG: Found {len(data)} campaigns.")
        
        for row in data:
            c_name = row.get('campaign_name', '')
            c_id = row.get('campaign_id')
            
            # Extract Metrics
            spend = float(row.get('spend', 0))
            
            # Revenue (action_values) - Priority Logic like entry.py
            rev = 0.0
            vals = row.get('action_values', [])
            
            def get_val(lst, type_key):
                 if not lst: return 0.0
                 for x in lst:
                     if x.get('action_type') == type_key: return float(x.get('value', 0))
                 return 0.0
            
            rev = get_val(vals, 'purchase') or get_val(vals, 'omni_purchase') or get_val(vals, 'offsite_conversion.fb_pixel_purchase') or get_val(vals, 'conversions_value')

            # Results (actions)
            acts = row.get('actions', [])
            res = get_val(acts, 'purchase') or get_val(acts, 'omni_purchase') or get_val(acts, 'offsite_conversion.fb_pixel_purchase') or get_val(acts, 'conversions')
                        
            roas = (rev / spend) if spend > 0 else 0
            cac = (spend / res) if res > 0 else 0
            
            if c_id not in all_campaigns_data:
                all_campaigns_data[c_id] = {"name": c_name}
            
            all_campaigns_data[c_id][label] = {
                "spend": spend,
                "revenue": rev,
                "results": res,
                "roas": roas,
                "cac": cac
            }

    await db.close()
    
    # Analyze Specific Campaign
    print("\n--- CAMPAIGN ANALYSIS ---")
    
    for c_id, data in all_campaigns_data.items():
        name = data.get('name', 'Unknown')
        if campaign_name_fragment not in name:
            continue
            
        print(f"\nCAMPAIGN: {name} (ID: {c_id})")
        
        curr = data.get("Current", {"spend": 0.0, "revenue": 0.0, "results": 0.0, "roas": 0.0, "cac": 0.0})
        prev = data.get("Previous", {"spend": 0.0, "revenue": 0.0, "results": 0.0, "roas": 0.0, "cac": 0.0})

        print(f"  Current Period: Spend=${curr['spend']:.2f}, Rev=${curr['revenue']:.2f}, Res={curr['results']}, ROAS={curr['roas']:.2f}, CAC=${curr['cac']:.2f}")
        print(f"  Previous Period: Spend=${prev['spend']:.2f}, Rev=${prev['revenue']:.2f}, Res={prev['results']}, ROAS={prev['roas']:.2f}, CAC=${prev['cac']:.2f}")
        
        # Deltas
        d_spend = curr['spend'] - prev['spend']
        d_rev = curr['revenue'] - prev['revenue']
        d_res = curr['results'] - prev['results']
        
        # % Change
        if prev['spend'] > 0:
            d_spend_pct = (d_spend / prev['spend']) * 100
        else: d_spend_pct = 100 if curr['spend'] > 0 else 0
            
        if prev['roas'] > 0:
            d_roas_pct = ((curr['roas'] - prev['roas']) / prev['roas']) * 100
        else: d_roas_pct = 0 # or infinity representation
            
        if prev['cac'] > 0:
            d_cac_pct = ((curr['cac'] - prev['cac']) / prev['cac']) * 100
        else: d_cac_pct = 0

        print(f"  DELTAS:")
        print(f"    Spend: ${d_spend:.2f} ({d_spend_pct:.1f}%)")
        print(f"    Revenue: ${d_rev:.2f}")
        print(f"    Results: {d_res}")
        print(f"    ROAS: {d_roas_pct:.2f}%")
        print(f"    CAC: {d_cac_pct:.2f}%")

if __name__ == "__main__":
    asyncio.run(debug_metrics())
