from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from meta.meta_curl import fetch_and_store, fetch_and_store_all, get_cached_insights as get_meta_insights
from google.google_curl import fetch_and_store as fetch_google, fetch_and_store_all as fetch_google_all, get_cached_insights as get_google_insights
from Database.database import DynamoDB
from contextlib import asynccontextmanager
import os
import requests
import urllib.parse
from dotenv import load_dotenv
from utils.security import encrypt_token
from utils.sync_tracker import SyncTracker


# Load environment variables
# Load environment variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'global.env')
load_dotenv(ENV_PATH, override=True)

# Meta OAuth Configuration
META_CLIENT_ID = os.getenv("META_CLIENT_ID", "").replace('"', '').replace("'", "").strip()
META_CLIENT_SECRET = os.getenv("META_CLIENT_SECRET", "").replace('"', '').replace("'", "").strip()
META_REDIRECT_URI = os.getenv("META_REDIRECT_URI", "http://localhost:8000/auth/meta/callback").replace('"', '').replace("'", "").strip()

# Google OAuth Configuration
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").replace('"', '').replace("'", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").replace('"', '').replace("'", "").strip()
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/google/callback").replace('"', '').replace("'", "").strip()
GOOGLE_DEVELOPER_TOKEN = os.getenv("GOOGLE_DEVELOPER_TOKEN", "").replace('"', '').replace("'", "").strip()

print(f"--- OAUTH CONFIG DIAGNOSTICS ---")
print(f"META_CLIENT_ID: {META_CLIENT_ID}")
print(f"META_REDIRECT_URI: {META_REDIRECT_URI}")
print(f"GOOGLE_CLIENT_ID: {GOOGLE_CLIENT_ID}")
print(f"GOOGLE_REDIRECT_URI: {GOOGLE_REDIRECT_URI}")
print(f"GOOGLE_DEVELOPER_TOKEN: {'SET' if GOOGLE_DEVELOPER_TOKEN else 'MISSING'}")
print(f"----------------------------------")




# Initialize Database instances
# We store integrations in one table and metrics in others
integrations_db = DynamoDB(table_name="Integrations")
sync_tracker = SyncTracker()

def init_db():
    print("Initializing database tables...")
    integrations_db.create_table(pk='platform', sk='account_id', sk_type='S')
    
    # Create metrics table and GSI for fast range queries
    metrics_db = DynamoDB(table_name="MetaAdsInsights")
    metrics_db.create_table(pk='campaign_id', sk='range_days', sk_type='N')
    metrics_db.create_range_days_gsi()  # Add GSI for ~100x faster queries

    # Create Google metrics table
    google_metrics_db = DynamoDB(table_name="GoogleAdsInsights")
    google_metrics_db.create_table(pk='campaign_id', sk='range_days', sk_type='N')
    google_metrics_db.create_range_days_gsi()

def cleanup():
    print("Shutting down...")

@asynccontextmanager
async def lifespan(app):
    init_db()
    yield
    cleanup()

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(lifespan=lifespan)

# CORS configuration
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000").replace('"', '').replace("'", "").strip()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://127.0.0.1:3000",
        FRONTEND_URL
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def health_check():
    return {"status": "ok", "message": "Backend is running"}

import threading



class IntegrationRequest(BaseModel):
    platform: str
    account_id: str
    email: str
    access_token: str


from meta.meta_curl import fetch_and_store, fetch_and_store_all, get_cached_insights

@app.get("/insights")
def get_insights(range: int = Query(7)):
    """
    Returns cached data from DynamoDB. Does NOT trigger a Meta API fetch.
    """
    return get_cached_insights(range)

@app.get("/insights/all")
def get_all_insights():
    """
    Returns all ranges (7, 30, 180 days) for both Meta and Google.
    """
    meta_7 = get_meta_insights(7)
    meta_30 = get_meta_insights(30)
    meta_180 = get_meta_insights(180)

    google_7 = get_google_insights(7)
    google_30 = get_google_insights(30)
    google_180 = get_google_insights(180)

    return {
        "7": meta_7 + google_7,
        "30": meta_30 + google_30,
        "180": meta_180 + google_180
    }

@app.get("/insights/sync-status")
def get_sync_status():
    """
    Returns current sync rate-limit status for the frontend.
    """
    return sync_tracker.get_status()

@app.post("/insights/sync")
def trigger_sync():
    """
    Triggers a fresh sync from Meta API and updates DynamoDB.
    Enforces a rate limit of MAX_SYNCS per COOLDOWN_HOURS window.
    """
    status = sync_tracker.get_status()

    if not status["can_sync"]:
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Sync limit reached ({status['max_syncs']}/{status['max_syncs']}). Please wait for cooldown.",
                "syncs_remaining": 0,
                "next_free_at": status["next_free_at"],
                "cooldown_seconds_remaining": status["cooldown_seconds_remaining"],
            }
        )

    def sync_with_tracking():
        """Wrapper that records the sync timestamp on success."""
        try:
            # Sync both platforms in background
            fetch_and_store_all()
            fetch_google_all()
            sync_tracker.record_sync()
        except Exception as e:
            print(f"Sync failed, not recording timestamp: {e}")

    threading.Thread(target=sync_with_tracking).start()

    return {
        "status": "started",
        "message": "Syncing data in background...",
        "syncs_remaining": status["syncs_remaining"] - 1,
    }

@app.get("/integrations")
def list_integrations(platform: Optional[str] = None):
    results = integrations_db.list_integrations(platform=platform)
    for res in results:
        # Fallback for older records missing account_name
        if 'account_name' not in res:
            res['account_name'] = res.get('account_id', 'Unknown Account')
            
        if 'access_token' in res:
            res['access_token'] = "********"  # Mask tokens for security
    return results


@app.post("/integrations")
def add_integration(req: IntegrationRequest):
    success = integrations_db.save_integration(
        platform=req.platform,
        account_id=req.account_id,
        email=req.email,
        access_token=encrypt_token(req.access_token)
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to save integration")
    return {"message": f"Successfully connected {req.platform} account {req.account_id}"}

@app.get("/auth/meta/login")
def meta_login():
    """Redirects to Facebook OAuth Dialog"""
    if not META_CLIENT_ID:
        raise HTTPException(status_code=500, detail="META_CLIENT_ID not configured")
    
    # Business apps need real permissions to trigger the dialog
    scope = "email,ads_read"
    # Use safe='' to encode EVERYTHING including // and :
    encoded_uri = urllib.parse.quote(META_REDIRECT_URI, safe='')
    url = f"https://www.facebook.com/v21.0/dialog/oauth?client_id={META_CLIENT_ID}&redirect_uri={encoded_uri}&scope={scope}"
    print(f"DEBUG: Generated Meta OAuth URL: {url}")
    return {"url": url}

from fastapi.responses import RedirectResponse

@app.get("/auth/meta/callback")
def meta_callback(code: str):
    """Handles OAuth callback and exchanges code for long-lived token"""
    if not code:
        raise HTTPException(status_code=400, detail="Code not provided")

    # 1. Exchange code for short-lived token
    token_url = "https://graph.facebook.com/v24.0/oauth/access_token"
    params = {
        "client_id": META_CLIENT_ID,
        "redirect_uri": META_REDIRECT_URI,
        "client_secret": META_CLIENT_SECRET,
        "code": code
    }
    r = requests.get(token_url, params=params)
    data = r.json()
    
    if "error" in data:
        return data

    short_token = data["access_token"]

    # 2. Exchange for long-lived token (60 days)
    ll_params = {
        "grant_type": "fb_exchange_token",
        "client_id": META_CLIENT_ID,
        "client_secret": META_CLIENT_SECRET,
        "fb_exchange_token": short_token
    }
    r_ll = requests.get(token_url, params=ll_params)
    ll_data = r_ll.json()
    long_token = ll_data.get("access_token")

    if not long_token:
        # Fallback if exchange fails
        long_token = short_token

    # 3. Fetch Ad Accounts for this user
    accounts_url = f"https://graph.facebook.com/v24.0/me/adaccounts"
    acc_r = requests.get(accounts_url, params={"access_token": long_token, "fields": "name,account_id"})
    accounts = acc_r.json().get("data", [])

    # 4. Save each account to Integrations table
    user_info = requests.get("https://graph.facebook.com/me", params={"access_token": long_token, "fields": "email"}).json()
    user_email = user_info.get("email", "N/A")

    for acc in accounts:
        integrations_db.save_integration(
            platform="meta",
            account_id=acc["account_id"],
            account_name=acc.get("name", f"Meta Account {acc['account_id']}"),
            email=user_email,
            access_token=encrypt_token(long_token)
        )


    # 5. Immediate sync for the new accounts
    try:
        fetch_and_store_all()
    except Exception as e:
        print(f"Post-login sync failed: {e}")

    # Redirect back to the frontend
    return RedirectResponse(url="http://localhost:3000/integrations?success=true&platform=meta")

@app.get("/auth/google/login")
def google_login():
    """Redirects to Google OAuth Dialog"""
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=500, detail="GOOGLE_CLIENT_ID not configured")
    
    # Scopes for Google Ads and email
    scope = "https://www.googleapis.com/auth/adwords email openid"
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": scope,
        "access_type": "offline",
        "prompt": "consent"
    }
    encoded_params = urllib.parse.urlencode(params)
    url = f"https://accounts.google.com/o/oauth2/v2/auth?{encoded_params}"
    print(f"DEBUG: Generated Google OAuth URL: {url}")
    return {"url": url}

@app.get("/auth/google/callback")
def google_callback(code: str):
    """Handles Google OAuth callback and exchanges code for tokens"""
    if not code:
        raise HTTPException(status_code=400, detail="Code not provided")

    # 1. Exchange code for tokens
    token_url = "https://oauth2.googleapis.com/token"
    payload = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
        "code": code
    }
    r = requests.post(token_url, data=payload)
    data = r.json()
    
    if "error" in data:
        return data

    access_token = data["access_token"]
    refresh_token = data.get("refresh_token") # Note: only provided on first consent or with prompt=consent

    # 2. Fetch User Email
    user_info_r = requests.get("https://www.googleapis.com/oauth2/v3/userinfo", 
                              params={"access_token": access_token})
    user_email = user_info_r.json().get("email", "N/A")

    # 3. Save to Integrations table
    # For Google, we use the email or a unique ID as account_id for now 
    # until we can list Google Ads accounts specifically
    integrations_db.save_integration(
        platform="google",
        account_id=user_email, # Standardizing on email for the connection itself
        account_name=f"Google Account ({user_email})",
        email=user_email,
        access_token=encrypt_token(refresh_token or access_token)
    )

    # Redirect back to the frontend
    return RedirectResponse(url="http://localhost:3000/integrations?success=true&platform=google")

