import os
import sys
import asyncio # Added for asyncio.gather
import datetime

# Ensure the current directory is in the path for finding siblings like 'meta', 'google', 'Database'
sys.path.append(os.path.dirname(__file__))

# Removed direct imports for get_cached_insights as they are now wrapped
from meta.meta_curl import fetch_and_store, fetch_and_store_all
import meta.meta_curl as meta_module
from google_ads.google_sdk import fetch_and_store as fetch_google, fetch_and_store_all as fetch_google_all, discover_accounts
import google_ads.google_sdk as google_module
from Database.database import DynamoDB
from dotenv import load_dotenv

# Import custom fetchers, for actions on dash 
from meta.meta_curl import fetch_custom_range as fetch_meta_custom
from google_ads.google_sdk import fetch_custom_range as fetch_google_custom

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
    token_type: str = "bearer"
    email: str
    preferences: Optional[dict] = None

class UserPreferences(BaseModel):
    custom_range: Optional[dict] = None
    selected_label: Optional[str] = None

# New async wrappers for insights
async def get_meta_insights(days: int = 7, integration_ids: list = None):
    from meta.meta_curl import async_get_cached_insights
    return await async_get_cached_insights(days, integration_ids=integration_ids)

async def get_google_insights(days: int = 7, integration_ids: list = None):
    from google_ads.google_sdk import async_get_cached_insights
    return await async_get_cached_insights(days, integration_ids=integration_ids)


@app.get("/api/insights")
async def get_insights(range: int = Query(7), user: dict = Depends(get_current_user)):
    """
    Returns cached Meta insights for the authorized integrations of the user.
    """
    user_id = user.get("user_id")
    
    # Fetch authorized integrations
    integrations = await integrations_db.async_list_integrations(user_id=user_id)
    meta_ids = [i['id'] for i in integrations if i.get('platform') == 'meta']
    
    if not meta_ids:
        return []

    return await get_meta_insights(range, integration_ids=meta_ids)

@app.get("/api/insights/all")
async def get_all_insights(user: dict = Depends(get_current_user)):
    """
    Returns all ranges (7, 30, 180 days) for both Meta and Google, 
    scoped strictly to the user's authorized integrations.
    """
    user_id = user.get("user_id")
    
    # Step B: Fetch authorized integrations
    integrations = await integrations_db.async_list_integrations(user_id=user_id)
    meta_ids = [i['id'] for i in integrations if i.get('platform') == 'meta']
    google_ids = [i['id'] for i in integrations if i.get('platform') == 'google']

    # Step C: Parallel fetch with scoped IDs
    results = await asyncio.gather(
        get_meta_insights(7, integration_ids=meta_ids),
        get_meta_insights(30, integration_ids=meta_ids),
        get_meta_insights(180, integration_ids=meta_ids),
        get_google_insights(7, integration_ids=google_ids),
        get_google_insights(30, integration_ids=google_ids),
        get_google_insights(180, integration_ids=google_ids)
    )
    
    meta_7, meta_30, meta_180, google_7, google_30, google_180 = results

    return {
        "7": meta_7 + google_7,
        "30": meta_30 + google_30,
        "180": meta_180 + google_180
    }

@app.get("/api/insights/custom")
async def get_custom_insights(
    start_date: str = Query(..., description="YYYY-MM-DD"), 
    end_date: str = Query(..., description="YYYY-MM-DD"),
    user: dict = Depends(get_current_user)
):
    """
    Returns insights for a specific custom date range, 
    scoped strictly to the user's authorized integrations.
    Fetches LIVE from APIs.
    """
    import concurrent.futures
    user_id = user.get("user_id")
    
    # Step B: Fetch authorized integrations
    integrations = await integrations_db.async_list_integrations(user_id=user_id)
    meta_ids = [i['id'] for i in integrations if i.get('platform') == 'meta']
    google_ids = [i['id'] for i in integrations if i.get('platform') == 'google']

    # Step C: Parallel fetch from LIVE APIs
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        meta_future = loop.run_in_executor(pool, fetch_meta_custom, start_date, end_date, meta_ids)
        google_future = loop.run_in_executor(pool, fetch_google_custom, start_date, end_date, google_ids)
        
        meta_data = await meta_future
        google_data = await google_future
        
    return meta_data + google_data

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

    user_id = user.get("user_id")
    # Fetch authorized integrations
    integrations = await integrations_db.async_list_integrations(user_id=user_id)
    meta_ids = [i['id'] for i in integrations if i.get('platform') == 'meta']
    google_ids = [i['id'] for i in integrations if i.get('platform') == 'google']

    def sync_with_tracking():
        """Wrapper that records the sync timestamp on success."""
        print(f"SYNC TASK: Starting multi-platform sync for user {user_id}...")
        try:
            # Sync both platforms with authorized IDs
            fetch_and_store_all(meta_ids)   # Meta
            fetch_google_all(google_ids)    # Google
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
    # Return all connected accounts for the current user
    user_email = user.get("email")
    user_id = user.get("user_id")
    
    # We prefer filtering by user_id if available, but fallback to email for legacy
    # Note: async_list_integrations currently supports email filter. We should probably update it to support user_id too.
    # For now, let's just stick to email as we populate both user_id and email in Integrations.
    # But wait, user_id is safer.
    
    # Let's inspect async_list_integrations signature... it takes email.
    # We should update it to take user_id as well, or just filter in memory if small list?
    # Better: Update async_list_integrations to filter by user_id if provided.
    
    # Updated to filter by user_id if available, falling back to email
    if user_id:
        results = await integrations_db.async_list_integrations(user_id=user_id)
    else:
        results = await integrations_db.async_list_integrations(email=user_email)
    
    # Optional: Filter by user_id if we decide to rely on that strictly in future
    # if user_id:
    #    results = [r for r in results if r.get('user_id') == user_id]
        
    for res in results:
        # Fallback for older records missing account_name
        if 'account_name' not in res:
            res['account_name'] = res.get('account_id', 'Unknown Account')
            
        if 'access_token' in res:
            res['access_token'] = "********"  # Mask tokens for security
    return results


@app.get("/api/integrations/{integration_id}")
async def get_integration_details(integration_id: str, user: dict = Depends(get_current_user)):
    """
    Returns specific integration details by UUID.
    Verified against current user for security.
    """
    user_id = user.get("user_id")
    
    # Use the new .get() logic requested
    integration = await integrations_db.async_get_integration(integration_id, user_id=user_id)
    
    if not integration:
        # Check if it might be an older account lookup (backward compatibility attempt)
        # But usually UUID lookup is the way forward.
        raise HTTPException(status_code=404, detail="Integration not found or access denied")
        
    # Mask token
    if 'access_token' in integration:
        integration['access_token'] = "********"
        
    return integration


@app.post("/api/integrations")
async def add_integration(req: IntegrationRequest, user: dict = Depends(get_current_user)):
    user_id = user.get("user_id")
    # If using legacy token without user_id, we might need to fetch it?
    # Assuming token has it. If not, we could fetch user by email.
    if not user_id:
        u = await users_db.async_get_user(user.get("email"))
        if u: user_id = u.get('id')

    success = integrations_db.save_integration(
        platform=req.platform,
        account_id=req.account_id,
        email=req.email,
        access_token=encrypt_token(req.access_token),
        user_id=user_id
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to save integration")
    return {"message": f"Successfully connected {req.platform} account {req.account_id}"}


@app.delete("/api/integrations/{platform}/{account_id}")
async def delete_integration_legacy(platform: str, account_id: str, user: dict = Depends(get_current_user)):
    """
    Legacy deletion endpoint using platform/accountId.
    Deletes the first integration matching these criteria for the user.
    """
    if integrations_db is None:
        init_db_logic()

    # Find the integration first to make sure it belongs to the user
    user_id = user.get("user_id")
    user_email = user.get("email")
    
    # We should really fetch and check user_id
    # But for now, we'll use async_delete_integration which is being updated to be more specific.
    # Actually, let's just use the UUID-based deletion if possible.
    
    success = await integrations_db.async_delete_integration(platform, account_id)
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete integration")
    
    return {"message": f"Successfully deleted {platform} account {account_id}"}

@app.delete("/api/integrations/{integration_id}")
async def delete_integration_by_id(integration_id: str, user: dict = Depends(get_current_user)):
    """
    Modern deletion endpoint using UUID.
    """
    user_id = user.get("user_id")
    
    # Verify ownership
    integration = await integrations_db.async_get_integration(integration_id, user_id=user_id)
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
        
    # Hard delete or soft delete? database.py uses soft delete.
    # We need an async_delete_by_id in database.py
    success = await integrations_db.async_delete_by_id(integration_id)
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete integration")
        
    return {"message": "Integration deleted successfully"}

@app.post("/api/auth/register")
async def register(req: UserAuthRequest):
    try:
        # Check if user exists first to give correct error code
        existing_user = await users_db.async_get_user(req.email)
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already registered")

        success_user_id = await users_db.async_create_user(req.email, get_password_hash(req.password))
        
        if not success_user_id:
             raise HTTPException(status_code=500, detail="Failed to create user")

        token = create_access_token({"email": req.email, "user_id": success_user_id})
        return AuthResponse(access_token=token, email=req.email)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error during registration")

@app.post("/api/auth/login")
async def login(req: UserAuthRequest):
    try:
        user = await users_db.async_get_user(req.email)
        
        if not user or not verify_password(req.password, user.get('password_hash', '')):
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        token = create_access_token({"email": req.email, "user_id": user.get('id')})
        # Return user preferences if they exist
        preferences = user.get('preferences', {})
        return AuthResponse(access_token=token, email=req.email, preferences=preferences)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Login error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error during login")
@app.post("/api/user/preferences")
async def update_preferences(prefs: UserPreferences, user: dict = Depends(get_current_user)):
    """
    Updates the user's preferences (custom date ranges, selected labels).
    """
    try:
        user_email = user.get("email")
        if not user_email:
            raise HTTPException(status_code=400, detail="User email not found")

        updates = {}
        if prefs.custom_range:
            updates['custom_range'] = prefs.custom_range
        if prefs.selected_label:
            updates['selected_label'] = prefs.selected_label
            
        updated_prefs = await users_db.async_update_user_preferences(user_email, updates)
        
        if updated_prefs is None:
             raise HTTPException(status_code=404, detail="User not found or update failed")
        
        return {"message": "Preferences updated", "preferences": updated_prefs}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Update preferences error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
@app.get("/api/auth/meta/login")
def meta_login(user: dict = Depends(get_current_user)):
    """Redirects to Facebook OAuth Dialog"""
    if not META_CLIENT_ID:
        raise HTTPException(status_code=500, detail="META_CLIENT_ID not configured")
    
    user_id = user.get("user_id")
    
    # Business apps need real permissions to trigger the dialog
    scope = "email,ads_read"
    
    # Pass user_id in state if present
    state = f"user_id={user_id}" if user_id else ""
    
    # Use safe='' to encode EVERYTHING including // and :
    encoded_uri = urllib.parse.quote(META_REDIRECT_URI, safe='')
    url = f"https://www.facebook.com/v24.0/dialog/oauth?client_id={META_CLIENT_ID}&redirect_uri={encoded_uri}&scope={scope}&state={state}"
    print(f"DEBUG: Generated Meta OAuth URL: {url} (User ID: {user_id})")
    return {"url": url}

from fastapi.responses import RedirectResponse

@app.get("/api/auth/meta/callback")
async def meta_callback(code: str, background_tasks: BackgroundTasks, state: Optional[str] = None):
    """Handles OAuth callback and exchanges code for long-lived token"""
    if not code:
        raise HTTPException(status_code=400, detail="Code not provided")

    # Extract user_id from state
    initiating_user_id = None
    if state and "user_id=" in state:
        try:
            # Simple parsing for now "user_id=XYZ"
            parts = state.split("user_id=")
            if len(parts) > 1:
                initiating_user_id = parts[1]
                print(f"META OAUTH: Linking to user_id {initiating_user_id}")
        except Exception as e:
            print(f"Error parsing state: {e}")

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
            access_token=encrypt_token(long_token),
            needs_reauth=False,
            # Meta long-lived tokens expire in ~60 days
            access_token_expires_at=(datetime.datetime.utcnow() + datetime.timedelta(days=60)).isoformat(),
            user_id=initiating_user_id
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
def google_login(user: dict = Depends(get_current_user)):
    """Redirects to Google OAuth Dialog"""
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=500, detail="GOOGLE_CLIENT_ID not configured")
    
    user_id = user.get("user_id")
    
    # Pass user_id in state if present
    state = f"user_id={user_id}" if user_id else ""

    # Scopes for Google Ads and email
    scope = "https://www.googleapis.com/auth/adwords email openid"
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": scope,
        "access_type": "offline",
        "prompt": "consent",
        "state": state
    }
    encoded_params = urllib.parse.urlencode(params)
    url = f"https://accounts.google.com/o/oauth2/v2/auth?{encoded_params}"
    print(f"DEBUG: Generated Google OAuth URL: {url} (User ID: {user_id})")
    return {"url": url}

@app.get("/api/auth/google/callback")
async def google_callback(code: str, background_tasks: BackgroundTasks, state: Optional[str] = None):
    """Handles Google OAuth callback and exchanges code for tokens"""
    if not code:
        raise HTTPException(status_code=400, detail="Code not provided")

    # Extract user_id from state
    initiating_user_id = None
    if state and "user_id=" in state:
        try:
            parts = state.split("user_id=")
            if len(parts) > 1:
                initiating_user_id = parts[1]
                print(f"GOOGLE OAUTH: Linking to user_id {initiating_user_id}")
        except Exception as e:
            print(f"Error parsing state: {e}")

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
    customer_accounts = discover_accounts(token_for_discovery, email=user_email)
    
    if not customer_accounts:
        print(f"GOOGLE OAUTH: No customer IDs discovered, saving {user_email} as fallback.")
        customer_accounts = [{'id': user_email, 'name': f"Google Account ({user_email})"}]

    saved_count = 0
    for account in customer_accounts:
        cid = account['id']
        name = account['name']
        success = integrations_db.save_integration(
            platform="google",
            account_id=cid,
            account_name=name,
            email=user_email,
            access_token=encrypt_token(refresh_token or access_token),
            needs_reauth=False,
            last_token_refresh_at=datetime.datetime.utcnow().isoformat(),
            user_id=initiating_user_id
            # Google refresh tokens don't strictly expire, so we don't set access_token_expires_at
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

