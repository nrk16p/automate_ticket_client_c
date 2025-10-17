import requests
import json
import os
import pandas as pd
import random
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ==================================
# Load Environment Variables
# ==================================
load_dotenv()

USERNAME  = os.getenv("CPAC_USERNAME")
PASSWORD  = os.getenv("CPAC_PASSWORD")
AUDIENT   = os.getenv("CPAC_AUDIENT")
SIGNATURE = os.getenv("CPAC_SIGNATURE")

TOKEN_FILE = "cpac_token.json"
AUTH_URL   = "https://api-cpac.scg.com/auth/oauth2/token"
REPORT_URL = "https://api-cpac.scg.com/e-suppliers/external/api/report-download/search"

# ==================================
# Modern 2025 User-Agent Pool
# ==================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; SM-G996U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]

# ==================================
# Step 1: Get or Load Token
# ==================================
def get_token():
    """Get access token from file or request a new one"""
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                token_data = json.load(f)
                # Reuse token if still valid (optional: check expires_in)
                if "access_token" in token_data:
                    return token_data["access_token"]
        except Exception:
            pass  # continue to request new token if file corrupt

    print("🔑 Requesting new access token...")
    auth = (USERNAME, PASSWORD)
    payload = {"grant_type": "client_credentials"}

    response = requests.post(AUTH_URL, data=payload, auth=auth)
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data.get("access_token")
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f, indent=2)
        print("✅ Token saved successfully.")
        return access_token
    else:
        raise Exception(f"❌ Failed to get token: {response.text}")


# ==================================
# Step 2: Fetch Report Data with Retry
# ==================================
def get_report(date_from: str, date_to: str, max_retries: int = 3):
    """Fetch report data from CPAC API with retry mechanism"""
    access_token = get_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-audient": AUDIENT,
        "x-signature": SIGNATURE,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/140.0.0.0 Safari/537.36",
        "Referer": "https://portal.cpac.co.th",
        "Accept": "application/json",
    }

    params = {"dateFrom": date_from, "dateTo": date_to}

    for attempt in range(1, max_retries + 1):
        headers["User-Agent"] = random.choice(USER_AGENTS)
        print(f"📡 Attempt {attempt}/{max_retries} | User-Agent: {headers['User-Agent'][:60]}...")

        try:
            response = requests.get(REPORT_URL, headers=headers, params=params, timeout=60)

            if response.status_code == 200:
                print("✅ Report data received successfully.")
                try:
                    data_json = response.json()
                    return data_json.get("data", [])
                except Exception as e:
                    raise Exception(f"❌ Failed to parse JSON response: {e}")

            else:
                print(f"⚠️ Failed ({response.status_code}): {response.text}")

        except requests.RequestException as e:
            print(f"⚠️ Network error: {e}")

        # Wait before retrying
        if attempt < max_retries:
            wait_time = 5 * attempt
            print(f"⏳ Retrying in {wait_time}s...")
            time.sleep(wait_time)

    raise Exception("❌ All retry attempts failed to fetch report data.")


# ==================================
# Step 3: Convert to DataFrame
# ==================================
def convert_to_dataframe(data: list) -> pd.DataFrame:
    """Convert JSON list to DataFrame"""
    df = pd.DataFrame(data)

    if not df.empty:
        if "dpDate" in df.columns:
            df["dpDate"] = pd.to_datetime(df["dpDate"], unit="ms", errors="coerce")
        if "dpTime" in df.columns:
            df["dpTime"] = pd.to_datetime(df["dpTime"], unit="ms", errors="coerce")

    print(f"✅ DataFrame created ({len(df)} rows).")
    return df


# ==================================
# Step 4: Main
# ==================================
if __name__ == "__main__":
    # 🕒 Yesterday’s date
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

    # 🔁 Fetch report
    try:
        data = get_report(yesterday, yesterday)
        df = convert_to_dataframe(data)

        # 💾 Save output
        os.makedirs("raw_data", exist_ok=True)
        if not df.empty:
            output_file = f"raw_data/cpac_{yesterday.replace('-', '')}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"✅ Saved report: {output_file}")
        else:
            print(f"⚠️ No data found for {yesterday}")

    except Exception as e:
        print(f"❌ Error: {e}")
