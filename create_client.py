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


def reset_machine(username):
    """Clear machine binding for a client so they can activate on a new machine."""
    url = f"{SUPABASE_URL}/rest/v1/capp_clients"
    with httpx.Client() as client:
        r = client.patch(
            url,
            params={"username": f"eq.{username}"},
            json={"seat_1_machine": None, "seat_2_machine": None},
            headers={**_supabase_headers(), "Content-Type": "application/json", "Prefer": "return=minimal"},
        )
    if r.status_code in (200, 204):
        print(f"\n✓ Machine binding cleared for '{username}'. They can now activate on a new machine.")
    else:
        print(f"\nERROR: {r.status_code} — {r.text}")


if __name__ == "__main__":
    print("=== CAPP Admin Tool ===\n")
    print("1. Create new client")
    print("2. Reset machine binding")
    choice = input("\nChoice (1 or 2): ").strip()

    if choice == "1":
        print()
        client_id = input("Client ID (short code, e.g. 'airforce'): ").strip().lower()
        username   = input("Username: ").strip()
        password   = input("Password: ").strip()
        is_admin   = input("Admin account? (y/n): ").strip().lower() == "y"

        if not client_id or not username or not password:
            print("All fields are required.")
            exit(1)

        payload = {
            "client_id": client_id,
            "username": username,
            "password_hash": __import__('hashlib').sha256((password + (salt := str(__import__('uuid').uuid4()))).encode()).hexdigest(),
            "salt": salt,
            "api_key": str(__import__('uuid').uuid4()),
            "active": True,
            "is_admin": is_admin,
        }
        url = f"{SUPABASE_URL}/rest/v1/capp_clients"
        with httpx.Client() as client:
            r = client.post(url, json=payload, headers=_supabase_headers())
        if r.status_code in (200, 201):
            print(f"\n✓ Client created!")
            print(f"  Client ID : {payload['client_id']}")
            print(f"  Username  : {username}")
            print(f"  Admin     : {is_admin}")
            print(f"  API Key   : {payload['api_key']}")
        else:
            print(f"\nERROR: {r.status_code} — {r.text}")

    elif choice == "2":
        print()
        username = input("Username to reset: ").strip()
        reset_machine(username)

    else:
        print("Invalid choice.")
