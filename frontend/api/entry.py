import os
import sys
import asyncio # Added for asyncio.gather

# Ensure the current directory is in the path for finding siblings like 'meta', 'google', 'Database'
sys.path.append(os.path.dirname(__file__))

# Removed direct imports for get_cached_insights as they are now wrapped
from meta.meta_curl import fetch_and_store, fetch_and_store_all
import meta.meta_curl as meta_module
from google_ads.google_sdk import fetch_and_store as fetch_google, fetch_and_store_all as fetch_google_all, discover_accounts
import google_ads.google_sdk as google_module
from Database.database import DynamoDB
from dotenv import load_dotenv

# Load environment variables (Local only)
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'global.env')
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH, override=True)

from utils.security import encrypt_token
from utils.sync_tracker import SyncTracker

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Depends
from pydantic import BaseModel
from typing import List, Optional
from contextlib import asynccontextmanager
import requests
import urllib.parse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from utils.security import verify_access_token, get_password_hash, verify_password, create_access_token

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    payload = verify_access_token(token)
    if not payload or "email" not in payload:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload

# Meta OAuth Configuration
META_CLIENT_ID = os.getenv("META_CLIENT_ID", "").replace('"', '').replace("'", "").strip()
META_CLIENT_SECRET = os.getenv("META_CLIENT_SECRET", "").replace('"', '').replace("'", "").strip()
META_REDIRECT_URI = os.getenv("META_REDIRECT_URI", "http://localhost:8000/api/auth/meta/callback").replace('"', '').replace("'", "").strip()

# Google OAuth Configuration
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").replace('"', '').replace("'", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").replace('"', '').replace("'", "").strip()
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/api/auth/google/callback").replace('"', '').replace("'", "").strip()
GOOGLE_DEVELOPER_TOKEN = os.getenv("GOOGLE_DEVELOPER_TOKEN", "").replace('"', '').replace("'", "").strip()

# print(f"--- OAUTH CONFIG DIAGNOSTICS ---")
# print(f"META_CLIENT_ID: {META_CLIENT_ID}")
# print(f"META_REDIRECT_URI: {META_REDIRECT_URI}")
# print(f"GOOGLE_CLIENT_ID: {GOOGLE_CLIENT_ID}")
# print(f"GOOGLE_REDIRECT_URI: {GOOGLE_REDIRECT_URI}")
# print(f"GOOGLE_DEVELOPER_TOKEN: {'SET' if GOOGLE_DEVELOPER_TOKEN else 'MISSING'}")
# print(f"----------------------------------")




# Global references for lazy init
integrations_db = None
users_db = None
sync_tracker = None

def init_db_logic():
    global integrations_db, users_db, sync_tracker
    
    # Initialize instances without creating tables
    # The API assumes tables are already created by the setup script
    print("DEBUG: Initializing Database...")
    integrations_db = DynamoDB(table_name="Integrations")
    users_db = DynamoDB(table_name="Users")
    sync_tracker = SyncTracker()
    print(f"DEBUG: integrations_db initialized: {integrations_db is not None}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    init_db_logic()
    
    # Initialize persistent async connections
    print("LIFESPAN: Connecting to databases...")
    if integrations_db: await integrations_db.connect()
    if users_db: await users_db.connect()
    if sync_tracker: 
        # SyncTracker might use DB internally? No, looks like in-memory or simple. 
        # Checked file listing, sync_tracker.py is there. Assuming it doesn't need async connect yet or uses its own.
        pass
        
    await meta_module.init_db()
    await google_module.init_db()
    
    yield
    
    # Shutdown logic
    print("LIFESPAN: Closing database connections...")
    if integrations_db: await integrations_db.close()
    if users_db: await users_db.close()
    await meta_module.close_db()
    await google_module.close_db()

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def catch_exceptions_middleware(request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        import traceback
        is_prod = os.getenv("VERCEL") or os.getenv("ENVIRONMENT") == "production"
        
        # Always log full error to console
        error_msg = f"Unhandled error: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        
        from fastapi.responses import JSONResponse
        
        content = {"detail": "Internal Server Error"}
        if not is_prod:
            # Only include details/traceback in non-production
            content["error"] = str(e)
            content["traceback"] = traceback.format_exc()
            
        return JSONResponse(
            status_code=500,
            content=content
        )

from fastapi.middleware.cors import CORSMiddleware

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

@app.get("/api/")
async def health_check():
    return {"status": "ok", "message": "Backend is running"}

# import threading



class IntegrationRequest(BaseModel):
    platform: str
    account_id: str
    email: str
    access_token: str

class UserAuthRequest(BaseModel):
    email: str
    password: str

class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    email: str

# New async wrappers for insights
async def get_meta_insights(days: int = 7):
    from meta.meta_curl import async_get_cached_insights
    return await async_get_cached_insights(days)

async def get_google_insights(days: int = 7):
    from google_ads.google_sdk import async_get_cached_insights
    return await async_get_cached_insights(days)


@app.get("/api/insights")
async def get_insights(range: int = Query(7), user: dict = Depends(get_current_user)):
    """
    Returns cached data from DynamoDB. Does NOT trigger a Meta API fetch.
    """
    # This endpoint now needs to decide which platform's insights to return,
    # or return a combined view. For now, let's assume it returns Meta insights
    # as it did before, but using the new async wrapper.
    # If a combined view is desired, this endpoint's logic would need to change.
    return await get_meta_insights(range)

@app.get("/api/insights/all")
async def get_all_insights(user: dict = Depends(get_current_user)):
    """
    Returns all ranges (7, 30, 180 days) for both Meta and Google.
    """
    # Fetch all ranges in parallel using asyncio.gather
    # Each call is now an async DB query via aioboto3
    results = await asyncio.gather(
        get_meta_insights(7),
        get_meta_insights(30),
        get_meta_insights(180),
        get_google_insights(7),
        get_google_insights(30),
        get_google_insights(180)
    )
    
    meta_7, meta_30, meta_180, google_7, google_30, google_180 = results

    return {
        "7": meta_7 + google_7,
        "30": meta_30 + google_30,
        "180": meta_180 + google_180
    }

@app.get("/api/insights/sync-status")
async def get_sync_status():
    """
    Returns current sync rate-limit status for the frontend.
    """
    return sync_tracker.get_status()

@app.post("/api/insights/sync")
async def trigger_sync(background_tasks: BackgroundTasks, user: dict = Depends(get_current_user)):
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
        print("SYNC TASK: Starting multi-platform sync...")
        try:
            # Sync both platforms
            fetch_and_store_all() # Meta
            fetch_google_all()    # Google
            sync_tracker.record_sync()
            print("SYNC TASK: Success.")
        except Exception as e:
            print(f"SYNC TASK FAILED: {e}")

    background_tasks.add_task(sync_with_tracking)

    return {
        "status": "started",
        "message": "Syncing data in background...",
        "syncs_remaining": status["syncs_remaining"] - 1,
    }

@app.get("/api/integrations")
async def get_integrations(user: dict = Depends(get_current_user)):
    print(f"DEBUG: get_integrations called. integrations_db is None? {integrations_db is None}")
    if integrations_db is None:
        print("DEBUG: integrations_db is NONE, re-initializing...")
        init_db_logic()
    # Return all connected accounts
    results = await integrations_db.async_list_integrations()
    for res in results:
        # Fallback for older records missing account_name
        if 'account_name' not in res:
            res['account_name'] = res.get('account_id', 'Unknown Account')
            
        if 'access_token' in res:
            res['access_token'] = "********"  # Mask tokens for security
    return results


@app.post("/api/integrations")
async def add_integration(req: IntegrationRequest, user: dict = Depends(get_current_user)):
    success = integrations_db.save_integration(
        platform=req.platform,
        account_id=req.account_id,
        email=req.email,
        access_token=encrypt_token(req.access_token)
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to save integration")
    return {"message": f"Successfully connected {req.platform} account {req.account_id}"}


@app.delete("/api/integrations/{platform}/{account_id}")
async def delete_integration(platform: str, account_id: str, user: dict = Depends(get_current_user)):
    """
    Deletes an integration for a specific platform and account ID.
    """
    if integrations_db is None:
        init_db_logic()

    success = await integrations_db.async_delete_integration(platform, account_id)
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete integration")
    
    return {"message": f"Successfully deleted {platform} account {account_id}"}

@app.post("/api/auth/register")
async def register(req: UserAuthRequest):
    # Check if user exists
    # DynamoDB.table.get_item(Key={'email': req.email})
    try:
        from boto3.dynamodb.conditions import Key
        response = await users_db.async_query(KeyConditionExpression=Key('email').eq(req.email))
        if response.get('Items'):
            raise HTTPException(status_code=400, detail="Email already registered")
        
        import datetime
        timestamp = datetime.datetime.utcnow().isoformat()
        
        await users_db.async_put_item(Item={
            'email': req.email,
            'password_hash': get_password_hash(req.password),
            'created_at': timestamp
        })
        
        token = create_access_token({"email": req.email})
        return AuthResponse(access_token=token, email=req.email)
    except Exception as e:
        print(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error during registration")

@app.post("/api/auth/login")
async def login(req: UserAuthRequest):
    try:
        from boto3.dynamodb.conditions import Key
        response = await users_db.async_query(KeyConditionExpression=Key('email').eq(req.email))
        items = response.get('Items', [])
        
        if not items:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        user = items[0]
        if not verify_password(req.password, user['password_hash']):
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        token = create_access_token({"email": req.email})
        return AuthResponse(access_token=token, email=req.email)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Login error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error during login")

@app.get("/api/auth/meta/login")
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

@app.get("/api/auth/meta/callback")
async def meta_callback(code: str, background_tasks: BackgroundTasks):
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

    saved_count = 0
    for acc in accounts:
        success = integrations_db.save_integration(
            platform="meta",
            account_id=acc["account_id"],
            account_name=acc.get("name", f"Meta Account {acc['account_id']}"),
            email=user_email,
            access_token=encrypt_token(long_token)
        )
        if success:
            saved_count += 1

    # 5. Immediate sync for the new accounts in background
    if saved_count > 0:
        background_tasks.add_task(fetch_and_store_all)
        print(f"META OAUTH: Added background sync task for {saved_count} accounts")

    # Redirect back to the frontend
    return RedirectResponse(url=f"{FRONTEND_URL}/integrations?success=true&platform=meta")

@app.get("/api/auth/google/login")
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

@app.get("/api/auth/google/callback")
async def google_callback(code: str, background_tasks: BackgroundTasks):
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
        print(f"GOOGLE OAUTH ERROR: {data}")
        return data

    access_token = data["access_token"]
    refresh_token = data.get("refresh_token") # Note: only provided on first consent or with prompt=consent

    # 2. Fetch User Email
    user_info_r = requests.get("https://www.googleapis.com/oauth2/v3/userinfo", 
                              params={"access_token": access_token})
    user_info = user_info_r.json()
    user_email = user_info.get("email", "N/A")

    print(f"GOOGLE OAUTH: Received callback for {user_email}")
    
    # 3. Save to Integrations table
    # We discover real Google Ads Customer IDs to match Meta's account-based strategy
    # The discovery function now uses the official SDK which expects a token for initialization (preferably refresh_token)
    token_for_discovery = refresh_token or access_token
    customer_ids = discover_accounts(token_for_discovery, email=user_email)
    
    if not customer_ids:
        print(f"GOOGLE OAUTH: No customer IDs discovered, saving {user_email} as fallback.")
        customer_ids = [user_email]

    saved_count = 0
    for cid in customer_ids:
        success = integrations_db.save_integration(
            platform="google",
            account_id=cid,
            account_name=f"Google Account ({cid})",
            email=user_email,
            access_token=encrypt_token(refresh_token or access_token)
        )
        if success:
            saved_count += 1
            print(f"GOOGLE OAUTH: Successfully saved integration for {cid}")

    if saved_count > 0:
        # 4. Trigger asynchronous sync
        background_tasks.add_task(fetch_google_all)
        print(f"GOOGLE OAUTH: Added background sync task for {saved_count} accounts")
    else:
        print(f"GOOGLE OAUTH: FAILED to save any integrations for {user_email}")

    # Redirect back to the frontend
    return RedirectResponse(url=f"{FRONTEND_URL}/integrations?success=true&platform=google")

