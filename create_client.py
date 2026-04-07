"""
CAPP Admin Tool
---------------
Provision new school accounts and manage existing ones.

Usage:
    python create_client.py

Menu options:
    1. Create new client   — generates credentials, inserts Supabase row, prints email template
    2. Reset machine seats — clears seat_1/seat_2 binding so a school can re-activate
    3. Deactivate client   — sets active=False, blocks all logins for that account
    4. List clients        — shows all accounts and their seat binding status
"""

import hashlib
import os
import secrets
import string
import sys

import httpx
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL or SUPABASE_SERVICE_KEY not set in .env")
    sys.exit(1)


def _headers():
    return {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/json",
    }


def _gen_password(length=16) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _gen_api_key() -> str:
    return secrets.token_hex(32)


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((password + salt).encode()).hexdigest()


def _check_existing(username: str) -> bool:
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}", "select": "username"},
        headers=_headers(),
    )
    return r.status_code == 200 and len(r.json()) > 0


# ── Option 1: Create new client ───────────────────────────────────────────────

def create_client():
    print("\n=== Create New Client ===\n")
    client_id = input("Client ID  (short code, e.g. 'airforce'): ").strip().lower()
    username  = input("Username   (same as client ID usually)  : ").strip().lower()
    school    = input("School name (e.g. 'Air Force Falcons')  : ").strip()
    pw_input  = input("Password   (press Enter to auto-generate): ").strip()
    admin_in  = input("Admin account? (y/N)                    : ").strip().lower()

    if not client_id or not username or not school:
        print("\nERROR: Client ID, username, and school name are required.")
        return

    if _check_existing(username):
        print(f"\nERROR: Username '{username}' already exists.")
        return

    password = pw_input if pw_input else _gen_password()
    is_admin = admin_in == "y"
    salt     = secrets.token_hex(16)
    pw_hash  = _hash_password(password, salt)
    api_key  = _gen_api_key()

    row = {
        "client_id":      client_id,
        "username":       username,
        "password_hash":  pw_hash,
        "salt":           salt,
        "api_key":        api_key,
        "active":         True,
        "is_admin":       is_admin,
        "seat_1_machine": None,
        "seat_2_machine": None,
    }

    r = httpx.post(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        json=row,
        headers={**_headers(), "Prefer": "return=minimal"},
    )

    if r.status_code not in (200, 201):
        print(f"\nERROR: {r.status_code} — {r.text}")
        return

    print("\n" + "=" * 60)
    print("  ACCOUNT CREATED — send these credentials to the customer")
    print("=" * 60)
    print(f"  School:         {school}")
    print(f"  Username:       {username}")
    print(f"  Activation Key: {password}")
    print(f"  Client ID:      {client_id}")
    if is_admin:
        print("  [ADMIN ACCOUNT — no seat limits]")
    print("=" * 60)
    print("\n--- EMAIL TEMPLATE ---\n")
    print(f"Your CAPP Video Coordinator Suite account is ready.\n")
    print(f"Open CAPP and enter the following when prompted:\n")
    print(f"  Username:       {username}")
    print(f"  Activation Key: {password}\n")
    print(f"Two seats are included with your license.")
    print(f"The first two machines you activate will be bound")
    print(f"to your account automatically.\n")
    print(f"Questions? Contact roger@cappvcs.com")
    print("\n--- END OF TEMPLATE ---")


# ── Option 2: Reset machine seats ─────────────────────────────────────────────

def reset_seats():
    print("\n=== Reset Machine Seats ===\n")
    username = input("Username to reset: ").strip().lower()
    if not username:
        return

    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}"},
        json={"seat_1_machine": None, "seat_2_machine": None},
        headers={**_headers(), "Prefer": "return=minimal"},
    )
    if r.status_code in (200, 204):
        print(f"\n  Seats cleared for '{username}'.")
        print("  They can now activate on up to 2 new machines.")
    else:
        print(f"\nERROR: {r.status_code} — {r.text}")


# ── Option 3: Deactivate client ───────────────────────────────────────────────

def deactivate_client():
    print("\n=== Deactivate Client ===\n")
    username = input("Username to deactivate: ").strip().lower()
    if not username:
        return

    confirm = input(f"Deactivate '{username}'? This blocks all logins. (y/N): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    r = httpx.patch(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"username": f"eq.{username}"},
        json={"active": False},
        headers={**_headers(), "Prefer": "return=minimal"},
    )
    if r.status_code in (200, 204):
        print(f"\n  '{username}' has been deactivated.")
    else:
        print(f"\nERROR: {r.status_code} — {r.text}")


# ── Option 4: List clients ────────────────────────────────────────────────────

def list_clients():
    print("\n=== All Clients ===\n")
    r = httpx.get(
        f"{SUPABASE_URL}/rest/v1/capp_clients",
        params={"select": "username,client_id,active,is_admin,seat_1_machine,seat_2_machine"},
        headers=_headers(),
    )
    if r.status_code != 200:
        print(f"ERROR: {r.status_code} — {r.text}")
        return

    clients = r.json()
    if not clients:
        print("  No clients found.")
        return

    print(f"  {'USERNAME':<20} {'CLIENT ID':<20} {'ACTIVE':<8} {'SEAT 1':<10} {'SEAT 2'}")
    print("  " + "-" * 72)
    for c in clients:
        s1 = "bound" if c.get("seat_1_machine") else "open"
        s2 = "bound" if c.get("seat_2_machine") else "open"
        active = "yes" if c.get("active") else "NO"
        admin  = " [admin]" if c.get("is_admin") else ""
        print(f"  {c['username']:<20} {c['client_id']:<20} {active:<8} {s1:<10} {s2}{admin}")
    print(f"\n  Total: {len(clients)} account(s)")


# ── Main menu ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n=== CAPP Admin Tool ===")
    print("1. Create new client")
    print("2. Reset machine seats")
    print("3. Deactivate client")
    print("4. List all clients")

    choice = input("\nChoice: ").strip()
    if   choice == "1": create_client()
    elif choice == "2": reset_seats()
    elif choice == "3": deactivate_client()
    elif choice == "4": list_clients()
    else: print("Invalid choice.")
