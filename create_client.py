"""
CAPP Client Admin Tool
----------------------
Run this script to add a new client to the CAPP system.

Usage:
    python create_client.py

You will be prompted for:
    - Client ID    (short code, e.g. "airforce", "bc", "vt")
    - Username     (what they type to log in)
    - Password     (what they type to log in)

The script will:
    1. Create the account in Supabase
    2. Generate an API key for this client
    3. Print the API key -- add it to Render's CAPP_API_KEYS env var
"""

import os
import uuid
import hashlib
import httpx

SUPABASE_URL = "https://ftyfxwsiihljrugzvcry.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ0eWZ4"
    "d3NpaWhsanJ1Z3p2Y3J5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjM3MTYxNSwiZX"
    "hwIjoyMDg3OTQ3NjE1fQ.iv0J2pC0gaX2ux_iKlEjbMdWaHaNVVw0-0sPpLu4Ytk"
)

def _supabase_headers():
    return {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/json",
    }

def create_client(client_id, username, password):
    salt = str(uuid.uuid4())
    password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
    api_key = str(uuid.uuid4())

    payload = {
        "client_id": client_id,
        "username": username,
        "password_hash": password_hash,
        "salt": salt,
        "api_key": api_key,
        "active": True,
    }

    url = f"{SUPABASE_URL}/rest/v1/capp_clients"
    with httpx.Client() as client:
        r = client.post(url, json=payload, headers=_supabase_headers())

    if r.status_code in (200, 201):
        print("\n✓ Client created successfully!")
        print(f"\n  Client ID : {client_id}")
        print(f"  Username  : {username}")
        print(f"  API Key   : {api_key}")
        print(f"\n  ACTION REQUIRED:")
        print(f"  Add this API key to Render → Environment → CAPP_API_KEYS")
        print(f"  (comma-separate if multiple keys exist)")
    else:
        print(f"\nERROR: Supabase returned {r.status_code}")
        print(r.text)


if __name__ == "__main__":
    print("=== CAPP Client Setup ===\n")
    client_id = input("Client ID (short code, e.g. 'airforce'): ").strip().lower()
    username   = input("Username (what client types to log in): ").strip()
    password   = input("Password (what client types to log in): ").strip()

    if not client_id or not username or not password:
        print("All fields are required.")
        exit(1)

    create_client(client_id, username, password)
