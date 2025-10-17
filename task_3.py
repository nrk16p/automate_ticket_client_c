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
url_vehicledaily = os.getenv("url_vehicledaily")
#header url
headers = {
    "Referer": url_vehicledaily,
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
    for fleet_group_id in ["1", "2"]:
        payload = {
            "fleet_group_id": fleet_group_id,
            "fleet_id": "",
            "t_date": yesterday,
            "num_of_day": "1",          # just 1 day
            "submit": "พิมพ์",
            "display_type": "multiple-day",
            "report_type": "vehicle.daily.transaction"
        }

        r = s.post(url, data=payload, headers=headers, verify=False, timeout=60)
        r.raise_for_status()

        with io.BytesIO(r.content) as f:
            df = pd.read_excel(f, sheet_name=0, dtype=str, skiprows=1)
            df["fleet_group_id"] = fleet_group_id   # track source
            all_results.append(df)

# Combine both into one DataFrame
final_df = pd.concat(all_results, ignore_index=True)

final_df = final_df[['วันที่','ฟลีท','แพลนท์','หัว','Unnamed: 7','Unnamed: 8','Unnamed: 13','Unnamed: 14','Unnamed: 15','สเตตัส','คนขับรถ']]
# กำหนด mapping ของชื่อคอลัมน์
rename_map = {
    "หัว": "ยี่ห้อ",
    "Unnamed: 7": "เบอร์รถ",
    "Unnamed: 8": "ทะเบียน",
    "Unnamed: 13": "รหัส",
    "Unnamed: 14": "ชื่อ",
    "Unnamed: 15": "เบอร์โทร"
}

# rename columns
final_df = final_df.rename(columns=rename_map)
final_df = final_df.dropna(subset=["วันที่"])
final_df["ทะเบียน"] = final_df["ทะเบียน"].str.replace("สบ.", "", regex=False)
final_df.to_excel("raw_data/vehicledaily.xlsx", index=False)
