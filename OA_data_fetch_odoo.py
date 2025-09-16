import os
import json
import base64
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import pytz

# --------- Config from Environment ---------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID = "1V0x5_DJn6bC1xzyMeBglzSeH-eDIWtKG4E5Cv3rwA_I"

# Decode Google Service Account credentials
creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64))
creds = Credentials.from_service_account_info(
    creds_json,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

# --------- Login ---------
def odoo_login():
    url = f"{ODOO_URL}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "db": ODOO_DB,
            "login": ODOO_USERNAME,
            "password": ODOO_PASSWORD
        },
        "id": 1
    }
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    return resp.json()['result']['uid']

# --------- Fetch Data ---------
def fetch_all_data(uid, company_id, batch_size=1000):
    all_records, offset = [], 0
    domain = ["&", ["sales_type", "=", "oa"], ["state", "=", "sale"]]
    specification = {
        "amount_invoiced": {},
        "buyer_name": {},
        "partner_id": {"fields": {"display_name": {}}},
        "name": {},
        "order_ref": {"fields": {"display_name": {}}},
        "user_id": {"fields": {"display_name": {}}},
        "pi_date": {},
        "date_order": {},
        "amount_total": {},
        "total_product_qty": {},
        "team_id": {"fields": {"display_name": {}}}
    }

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "sale.order",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "domain": domain,
                    "specification": specification,
                    "offset": offset,
                    "limit": batch_size,
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": uid,
                        "allowed_company_ids": [company_id],
                        "bin_size": True,
                        "current_company_id": company_id
                    },
                    "count_limit": 10001
                }
            },
            "id": 2
        }
        resp = session.post(f"{ODOO_URL}/web/dataset/call_kw/sale.order/web_search_read", data=json.dumps(payload))
        resp.raise_for_status()
        result = resp.json()['result']
        records = result['records']
        all_records.extend(records)
        print(f"[Company {company_id}] Fetched {len(records)} records, total so far: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size

    print(f"✅ Company {company_id} total records fetched: {len(all_records)}")
    return all_records

# --------- Safe Getter ---------
def safe_get(obj, key, default=''):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

# --------- Flatten ---------
def flatten_record(rec):
    return {
        "Already Invoiced": rec.get("amount_invoiced", ""),
        "Buyer": rec.get("buyer_name", ""),
        "Customer": safe_get(rec.get("partner_id"), "display_name"),
        "Order Reference": rec.get("name", ""),
        "Sales Order Ref.": safe_get(rec.get("order_ref"), "display_name"),
        "Salesperson": safe_get(rec.get("user_id"), "display_name"),
        "PI Date": rec.get("pi_date", ""),
        "Order Date": rec.get("date_order", ""),
        "Total": rec.get("amount_total", ""),
        "Total PI Quantity": rec.get("total_product_qty", ""),
        "Sales Team": safe_get(rec.get("team_id"), "display_name")
    }

# --------- Upload to Google Sheet ---------
    
    
def paste_to_gsheet(df, sheet_name):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
    if df.empty:
        print(f"Skip: {sheet_name} DataFrame is empty, not pasting.")
        return

    # Clear only the range A:N
    worksheet.batch_clear(["A:N"])

    # Paste the dataframe
    set_with_dataframe(worksheet, df)

    print(f"✅ Data pasted to Google Sheet ({sheet_name}).")

    # Add timestamp to N1 using named arguments (new gspread)
    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update(range_name="N1", values=[[local_time]])
    print(f"Timestamp written to N1: {local_time}")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    company_map = [(1, "OA_RAW_DATA_ZIP"), (3, "OA_RAW_DATA_MT")]

    for company_id, sheet_tab in company_map:
        # Fetch data from Odoo
        records = fetch_all_data(uid, company_id)
        # Flatten records for Google Sheet
        flat_records = [flatten_record(r) for r in records]
        df = pd.DataFrame(flat_records)
        # Paste entire DataFrame at once to Google Sheet
        paste_to_gsheet(df, sheet_tab)
