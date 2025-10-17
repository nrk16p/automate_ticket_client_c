import requests
import io
import pandas as pd
import warnings, urllib3, requests, io
import os

from dotenv import load_dotenv

load_dotenv()

#token for get data from atsm 
PHPSESSID = os.getenv("PHPSESSID")
#data url
url_shipto = os.getenv("url_shipto")
#header url
headers = {
    "Referer": url_shipto,
    "Content-Type": "application/x-www-form-urlencoded",
    "Cookie": f"PHPSESSID={PHPSESSID}",
}
from datetime import datetime, timedelta

# Always yesterday
yesterday = (datetime.today() - timedelta(days=1)).strftime("%d/%m/%Y")

# Disable SSL warnings
warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)

all_results = []

with requests.Session() as s:
    for customer_id in ["139"]:
        payload = {
            "customer_id": customer_id,
            "status": "A",
            "from_updated_date":"01/01/2025",
            "submit": "พิมพ์",
            "display_type": "multiple-day",
            "report_type": "ship.to"
        }

        r = s.post(url, data=payload, headers=headers, verify=False, timeout=60000)
        r.raise_for_status()

        with io.BytesIO(r.content) as f:
            df = pd.read_excel(f, sheet_name=0, dtype=str, skiprows=1)
            df["customer_id"] = customer_id   # track source
            all_results.append(df)

# Combine both into one DataFrame
shipto = pd.concat(all_results, ignore_index=True)

shipto.to_excel("raw_data/shipto.xlsx", index=False)
