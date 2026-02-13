import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load environment variables
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'global.env')
load_dotenv(ENV_PATH, override=True)

# Get the encryption key from environment
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")

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
        print("CRITICAL: ENCRYPTION_KEY not found in global.env")
        return token
    return f.encrypt(token.encode()).decode()

def decrypt_token(encrypted_token: str) -> str:
    """Decrypts an encrypted token. Returns plain text tokens unchanged."""
    f = get_fernet()
    if not f:
        print("CRITICAL: ENCRYPTION_KEY not found in global.env")
        return encrypted_token
    
    # Check if token is already plain text (not encrypted)
    # Fernet tokens always start with 'gAAAAAB'
    if not encrypted_token.startswith('gAAAAAB'):
        # Token is plain text, return as-is
        return encrypted_token
    
    try:
        return f.decrypt(encrypted_token.encode()).decode()
    except Exception as e:
        print(f"Error decrypting token: {e}")
        # If decryption fails, assume it's plain text
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
