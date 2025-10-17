import requests
import warnings
import urllib3
import pandas as pd
import logging
from pathlib import Path
from typing import Optional
import os

from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────
warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)

BASE_URL = (
    "https://www.mena-atms.com/veh/vehicle/index.export/"
    "?page=1&order_by=v.code%20asc&search-toggle-status=&order_by=v.code%20asc"
)

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ── MAIN FUNCTION ─────────────────────────────────────────────────────────────
def run_vehiclemaster(phpsessid: str) -> Optional[pd.DataFrame]:
    """
    Fetch vehicle master table from mena-atms and save as JSON.
    Returns the cleaned DataFrame.
    """
    # --- Path detection ---
    # --- Path detection (serverless vs local) ---
    if Path("/tmp").exists():
        save_dir = Path("/tmp") / "data"
    else:
        save_dir = Path.cwd() / "data"
    save_dir.mkdir(parents=True, exist_ok=True)

    headers = {
        "Referer": BASE_URL,
        "Cookie": f"PHPSESSID={phpsessid}",
        "User-Agent": "Mozilla/5.0",
    }

    try:
        resp = requests.get(BASE_URL, headers=headers, verify=False, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch vehicle master: {e}")
        return None

    # Ensure proper encoding (Thai pages may omit charset)
    if not resp.encoding:
        resp.encoding = resp.apparent_encoding

    # Parse all tables
    try:
        tables = pd.read_html(resp.text, displayed_only=False)
    except ValueError:
        logging.error("No HTML tables found in vehicle master response.")
        return None

    if not tables:
        logging.warning("No tables parsed from vehicle master HTML.")
        return None

    # Pick largest table
    df = max(tables, key=lambda t: t.shape[0] * t.shape[1])

    # Clean columns
    df.columns = df.columns.map(lambda c: str(c).strip())
    df = df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", case=False)]
    df = df.astype(str)

    # Keep important columns (if present)
    keep_cols = ["ทะเบียน", "เลขรถ", "ประเภทรถร่วม", "ประเภทยานพาหนะ", "ประเภทยานพาหนะเพิ่มเติม"]
    existing_cols = [col for col in keep_cols if col in df.columns]
    df = df[existing_cols]

    # Save to excel

    
    df.to_excel("raw_data/vehiclemaster.xlsx", index=False)


    return df
phpsessid = os.getenv("PHPSESSID")

run_vehiclemaster(phpsessid)
