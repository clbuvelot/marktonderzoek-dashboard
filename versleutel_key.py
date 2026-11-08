"""
versleutel_key.py
-----------------
Versleutelt je Azure AI Foundry API key met een wachtwoord (AES-256-GCM).

Gebruik:
    pip install pycryptodome
    python versleutel_key.py
"""

import base64
import hashlib
import os

try:
    from Crypto.Cipher import AES
except ImportError:
    print("Installeer eerst: pip install pycryptodome")
    exit(1)


def versleutel(api_key, wachtwoord):
    salt = os.urandom(16)
    aes_key = hashlib.pbkdf2_hmac("sha256", wachtwoord.encode(), salt, 100_000, dklen=32)
    iv = os.urandom(12)
    cipher = AES.new(aes_key, AES.MODE_GCM, nonce=iv)
    ciphertext, tag = cipher.encrypt_and_digest(api_key.encode())
    payload = salt + iv + tag + ciphertext
    return base64.b64encode(payload).decode()


print("=== API Key Versleutelen ===\n")
api_key = input("Azure AI Foundry API key: ").strip()
if len(api_key) < 20:
    print("Dat lijkt geen geldige key — te kort")
    exit(1)

wachtwoord = input("Wachtwoord voor collega's: ").strip()
if len(wachtwoord) < 4:
    print("Wachtwoord te kort — minimaal 4 tekens")
    exit(1)

payload = versleutel(api_key, wachtwoord)

print(f"\n=== Resultaat ===")
print(f"Plak dit in dashboard-chat.js:\n")
print(f'const ENCRYPTED_KEY = "{payload}";')
print(f"\nCollega's loggen in met: {wachtwoord}")
