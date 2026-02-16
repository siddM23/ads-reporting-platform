import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load environment variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 'global.env')
load_dotenv(ENV_PATH, override=True)

# Get the encryption key from environment
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
JWT_ALGORITHM = "HS256"

import jwt
import bcrypt
from datetime import datetime, timedelta

def verify_password(plain_password: str, hashed_password: str):
    """Verifies a plain text password against its hash using direct bcrypt."""
    try:
        # bcrypt.checkpw expects bytes
        return bcrypt.checkpw(
            password=plain_password.encode('utf-8'),
            hashed_password=hashed_password.encode('utf-8')
        )
    except Exception as e:
        print(f"Error during password verification: {e}")
        return False

def get_password_hash(password: str):
    """Generates a salt and hashes a password using direct bcrypt."""
    # salt = bcrypt.gensalt() expects bytes, hashpw returns bytes
    hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    return hashed.decode('utf-8')

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=60) # 60 minutes default
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt

def verify_access_token(token: str):
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.PyJWTError:
        return None

def get_fernet():
    if not ENCRYPTION_KEY:
        # We don't raise an error here to prevent the app from crashing on start if just generating key
        # but the app will fail if it tries to encrypt/decrypt without it.
        return None
    return Fernet(ENCRYPTION_KEY.encode())

def encrypt_token(token: str) -> str:
    """Encrypts a plain text token."""
    f = get_fernet()
    if not f:
        # Check if we are in production
        is_prod = os.getenv("VERCEL") or os.getenv("ENVIRONMENT") == "production"
        if is_prod:
            raise ValueError("CRITICAL: ENCRYPTION_KEY is missing in production. Token will not be saved unencrypted.")
        print("WARNING: ENCRYPTION_KEY not found. Proceeding with plain text (Local only).")
        return token
    return f.encrypt(token.encode()).decode()

def decrypt_token(encrypted_token: str) -> str:
    """Decrypts an encrypted token. Returns plain text tokens unchanged."""
    f = get_fernet()
    if not f:
        is_prod = os.getenv("VERCEL") or os.getenv("ENVIRONMENT") == "production"
        if is_prod:
            raise ValueError("CRITICAL: ENCRYPTION_KEY is missing in production. Cannot decrypt tokens.")
        print("WARNING: ENCRYPTION_KEY not found. Returning token as-is (Local only).")
        return encrypted_token
    
    # Check if token is already plain text (not encrypted)
    # Fernet tokens always start with 'gAAAAAB'
    if not encrypted_token.startswith('gAAAAAB'):
        return encrypted_token
    
    try:
        return f.decrypt(encrypted_token.encode()).decode()
    except Exception as e:
        print(f"Error decrypting token: {e}")
        return encrypted_token

if __name__ == "__main__":
    # Script to generate a new key if run directly
    print("Generating new Fernet key...")
    print(Fernet.generate_key().decode())
