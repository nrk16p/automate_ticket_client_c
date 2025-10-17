#import libray
import pandas as pd 

#import data

#import cpac data 
cpac = pd.read_excel("raw_data/cpac.xlsx")

#import fleetlink
fleetlink = pd.read_json("raw_data/fleetlink.json")

#import vehicledaily
vehicledaily = pd.read_excel("raw_data/vehicledaily.xlsx")

#import vehiclemaster 
vehiclemaster = pd.read_excel("raw_data/vehiclemaster.xlsx")

#import ship to data 
shipto = pd.read_excel("raw_data/shipto.xlsx")

#cpac data process
#select col 
cpac = cpac[['plantNo',
       'dpNo', 'dpDate', 'dpTime', 'carNo', 'driverName',
       'siteCode', 'siteName', 'quantity','distanceCode']]

#convert dpDate & dptime to datetime format and use bkk timezone 
if "dpDate" in cpac.columns:
    cpac["dpDate"] = pd.to_datetime(cpac["dpDate"], unit="ms", utc=True).dt.tz_convert("Asia/Bangkok")

if "dpTime" in cpac.columns:
    cpac["dpTime"] = pd.to_datetime(cpac["dpTime"], unit="ms", utc=True).dt.tz_convert("Asia/Bangkok")
cpac

cpac["แพล้นท์"] = cpac["dpNo"].astype(str).str[:4]
cpac["วันที่"] = cpac["dpTime"]
cpac["วันที่ครบกำหนด"] = cpac["dpTime"]
cpac["เลขที่ตั๋วเพิ่ม 1"] = cpac["plantNo"]
cpac["เลขที่ตั๋วเพิ่ม 2"] = cpac["distanceCode"]
cpac["เวลาออกเดินทาง"] = cpac["dpTime"]
cpac["วันเวลาอ้างอิง 1"] = cpac["dpTime"]
cpac["วันเวลาอ้างอิง 2"] = cpac["dpTime"]
cpac["วันเวลาอ้างอิง 3"] = cpac["dpTime"]
cpac["วันเวลาอ้างอิง 4"] = cpac["dpTime"]
cpac["วันเวลาลงสินค้า"] = cpac["dpTime"]
cpac["วันเวลาปิด LDT"] = cpac["dpTime"]
cpac["นน ต้นทาง"] = cpac["quantity"]
cpac["LDT"] = cpac["dpNo"]
cpac["Ship To"] = cpac["dpNo"].astype(str).str[:4] + cpac["siteCode"]



#create new col and inputdata 
cpac["Type"] = "single drop"
cpac["ผลิตภัณฑ์"] = "คอนกรีตผสมเสร็จ"
cpac["dropoffs"] = ""
cpac["ประเภทวิ่ง"] = "legacy"
cpac["ประเภทการขนส่งขากลับ"] = ""
cpac["ประเภทการขนส่ง"] = "heavy"
cpac["Service Parameter A"] = ""
cpac["Service Parameter B"] = ""
cpac["แพล้นท์โอเนย้าย"] = ""
cpac["ผู้จัดส่งร่วม"] = ""
cpac["ทะเบียนหาง"] = ""
cpac["แพล้นท์โอเนย้าย"] = ""
cpac["หมายเหตุ"] = ""
cpac["วิ่งแทนรถทะเบียน"] = ""
cpac["สาขา"] = "LB"

#fleetlink data process

#select col
fleetlink = fleetlink[['หมายเลข DP', 'เวลาถึงไซต์งาน','เวลาออกจากไซต์งาน']]

# แปลงวันที่โดยให้ pandas เดารูปแบบอัตโนมัติ
for col in ['เวลาถึงไซต์งาน', 'เวลาออกจากไซต์งาน']:
    fleetlink[col] = pd.to_datetime(
        fleetlink[col], 
        errors='coerce',              # ถ้าแปลงไม่ได้ให้เป็น NaT
        infer_datetime_format=True    # ให้ pandas เดารูปแบบวันที่
    ).dt.tz_localize('Asia/Bangkok', ambiguous='NaT', nonexistent='NaT')

# แปลงให้เป็น string ก่อน merge
cpac["dpNo"] = cpac["dpNo"].astype(str)
fleetlink["หมายเลข DP"] = fleetlink["หมายเลข DP"].astype(str)
cpac["carNo"] = cpac["carNo"].astype(str)
vehicledaily["เบอร์รถ"] = vehicledaily["เบอร์รถ"].astype(str)

# รวมข้อมูลแบบ inner join
merged = cpac.merge(
    fleetlink,
    how="inner",
    left_on="dpNo",
    right_on="หมายเลข DP"
)


# รวมข้อมูล cpac and vehicledaily
merged = merged.merge(
    vehicledaily,
    how="inner",
    left_on="carNo",
    right_on="เบอร์รถ"
)


#calculate ประเภทรถร่วม from vehicledaily
merged["ประเภทรถร่วม"] = ""
# Rule 1: If 'ประเภท' == 'Scco MS', set 'นน ปลายทาง' to 1
merged.loc[merged['คนขับรถ'] == 'พจส', 'ประเภทรถร่วม'] = 'OT-MT01'
merged.loc[merged['คนขับรถ'] == 'พจร', 'ประเภทรถร่วม'] = 'OT-MT02'

merged["รหัส พจส 1"] = ""
merged["รหัส พจส 2"] = ""
merged["ทะเบียนหัว"] = ""

merged["รหัส พจส 1"] = merged["รหัส"]
merged["ทะเบียนหัว"] = "สบ." + merged["ทะเบียน"]

# รวมข้อมูลแบบ cpac and vehiclemaster 
merged = merged.merge(
    vehiclemaster,
    how="inner",
    left_on="carNo",
    right_on="เลขรถ"
)
merged

#cal new col by using data from vehiclemaster 
merged['เส้นทาง'] = merged['ประเภทยานพาหนะ'].apply(lambda x: 'CPAC L' if x == 'Mixer 10 ล้อ' else '6 ล้อ')
merged['บริการ'] = merged['เส้นทาง'].apply(lambda x: 'M026' if x == '6 ล้อ' else 'M025 ')


#convert data type
merged["Ship To"] = merged["Ship To"].astype(str)
shipto["รหัส"] = shipto["รหัส"].astype(str)


# รวมข้อมูล cpac and shipto
merged = merged.merge(
    shipto,
    how="left",
    left_on="Ship To",
    right_on="รหัส"
)


#cal col by using data from shipto
merged['นน ปลายทาง'] = merged['นน ต้นทาง']

merged['นน ปลายทาง'] = merged.apply(
    lambda row: 3 if (
        row['โซนการจัดส่ง'] == 'West' and 
        isinstance(row['นน ปลายทาง'], (int, float)) and 
        row['นน ปลายทาง'] <= 3
    ) else row['นน ปลายทาง'],
    axis=1
)

#convert date format
# 🔍 ตรวจหาคอลัมน์ที่เป็น datetime
datetime_cols = merged.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns

# 🕐 แปลงรูปแบบวันที่ทั้งหมดให้เป็น 'dd/mm/YYYY HH:MM'
for col in datetime_cols:
    merged[col] = merged[col].dt.strftime('%d/%m/%Y %H:%M')
    
    
# ✅ select col
selected_cols = [
    'สาขา', 'บริการ', 'LDT', 'Type', 'ผลิตภัณฑ์', 'Ship To', 'เส้นทาง', 'dropoffs',
    'ประเภทวิ่ง', 'ประเภทการขนส่งขากลับ', 'ประเภทการขนส่ง',
    'Service Parameter A', 'Service Parameter B', 'แพล้นท์', 'แพล้นท์โอเนย้าย',
    'วันที่', 'วันที่ครบกำหนด', 'เลขที่ตั๋วเพิ่ม 1', 'เลขที่ตั๋วเพิ่ม 2',
    'เลขที่ตั๋วเพิ่ม 3', 'เลขที่ตั๋วเพิ่ม 4', 'หมายเหตุ', 'ประเภทรถร่วม',
    'ผู้จัดส่งร่วม', 'รหัส พจส 1', 'รหัส พจส 2', 'ทะเบียนหัว', 'ทะเบียนหาง',
    'เวลาออกเดินทาง', 'นน ต้นทาง', 'นน ปลายทาง',
    'วันเวลาอ้างอิง 1', 'วันเวลาอ้างอิง 2', 'วันเวลาอ้างอิง 3',
    'วันเวลาอ้างอิง 4', 'วันเวลาลงสินค้า', 'วันเวลาปิด LDT', 'วิ่งแทนรถทะเบียน'
]

# ✅ ถ้ามีบางคอลัมน์ยังไม่มี เช่น "วันที่" อาจอยู่ในชื่ออื่น ("วันที่_x")
# ให้ใช้ rename ก่อน เช่น:
merged = merged.rename(columns={
    'วันที่_x': 'วันที่',
    "เวลาถึงไซต์งาน":"เลขที่ตั๋วเพิ่ม 3",
    "เวลาออกจากไซต์งาน":"เลขที่ตั๋วเพิ่ม 4",
    "ประเภทรถร่วม_x":"ประเภทรถร่วม"
})

# ✅ สร้าง DataFrame ใหม่ตามลำดับคอลัมน์
merged_selected = merged[selected_cols]


# 🔄 แปลงคอลัมน์ "วันที่" ให้เป็น datetime ก่อน (ignore error กรณีแปลงไม่ได้)
merged_selected["วันที่"] = pd.to_datetime(merged_selected["วันที่"], errors='coerce')

# 🕐 แปลงให้เป็นรูปแบบ 19/08/2025 08:04
merged_selected["วันที่"] = merged_selected["วันที่"].dt.strftime('%d/%m/%Y')

# 🔄 แปลงคอลัมน์ "วันที่" ให้เป็น datetime ก่อน (ignore error กรณีแปลงไม่ได้)
merged_selected["วันที่ครบกำหนด"] = pd.to_datetime(merged_selected["วันที่ครบกำหนด"], errors='coerce')

# 🕐 แปลงให้เป็นรูปแบบ 19/08/2025 08:04
merged_selected["วันที่ครบกำหนด"] = merged_selected["วันที่ครบกำหนด"].dt.strftime('%d/%m/%Y')

from datetime import datetime

# 🕒 Generate dynamic filename
today_str = datetime.now().strftime("%d-%m-%y")
filename = f"LDTCPAC_{today_str}.xlsx"

# 💾 Save file
merged_selected.to_excel(filename, index=False)

