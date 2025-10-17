import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ==================================
# Load Environment Variables
# ==================================
USERNAME  = os.getenv("CPAC_USERNAME")
PASSWORD  = os.getenv("CPAC_PASSWORD")
AUDIENT   = os.getenv("CPAC_AUDIENT")
SIGNATURE = os.getenv("CPAC_SIGNATURE")

TOKEN_FILE = os.getenv("TOKEN_FILE")
AUTH_URL   = os.getenv("AUTH_URL")
REPORT_URL = os.getenv("REPORT_URL")

# Debug: confirm loaded (Jenkins masks in logs automatically)
print(f"USERNAME: {USERNAME}")
print(f"PASSWORD: {PASSWORD}")
print(f"AUDIENT: {AUDIENT}")
print(f"SIGNATURE: {SIGNATURE}")


# ==================================
# Step 1: Get or Load Token
# ==================================
def get_token():
    payload = {
        "grant_type": "client_credentials",
        "client_id": AUDIENT,
        "client_secret": SIGNATURE
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    print(f"Requesting token from {AUTH_URL} ...")

    response = requests.post(AUTH_URL, data=payload, headers=headers)
    if response.status_code == 200:
        token = response.json().get("access_token")
        if token:
            print("✅ Token retrieved successfully.")
            # Optional: save to file if needed
            if TOKEN_FILE:
                with open(TOKEN_FILE, "w") as f:
                    f.write(token)
            return token
        else:
            raise Exception("❌ Token not found in response JSON.")
    else:
        raise Exception(f"❌ Failed to get token: {response.status_code} - {response.text}")


# ==================================
# Step 2: Fetch Report Data
# ==================================
def get_report(date_from: str, date_to: str):
    """Fetch report data from CPAC API"""
    access_token = get_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-audient": AUDIENT,
        "x-signature": SIGNATURE,
    }

    params = {
        "dateFrom": date_from,
        "dateTo": date_to
    }

    print("📡 Requesting report data...")
    response = requests.get(REPORT_URL, headers=headers, params=params)

    if response.status_code == 200:
        print("✅ Report data received successfully.")
        try:
            data_json = response.json()
            return data_json.get("data", [])
        except Exception:
            raise Exception("❌ Failed to parse JSON response")
    else:
        raise Exception(f"❌ Failed to fetch report ({response.status_code}): {response.text}")


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
# Example Usage
# ==================================

if __name__ == "__main__":
    # 🕒 Calculate yesterday’s date
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

    # 🔁 Use yesterday for both start and end date
    data = get_report(yesterday, yesterday)
    df = convert_to_dataframe(data)

    # 💾 Save file if data exists
    if not df.empty:
        df.to_excel("raw_data/cpac.xlsx", index=False)
        print(f"✅ Saved report for {yesterday}")
    else:
        print(f"⚠️ No data found for {yesterday}")
