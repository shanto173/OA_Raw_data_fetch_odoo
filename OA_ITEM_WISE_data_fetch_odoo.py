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

# Setup session
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

# --------- Login ---------
def odoo_login():
    """
    Login to Odoo using environment variables and return the UID.
    """
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
    result = resp.json().get('result')
    if not result or 'uid' not in result:
        raise Exception("❌ Odoo login failed, check credentials or URL")
    uid = result['uid']
    print(f"✅ Logged in to Odoo! UID: {uid}")
    return uid

# --------- Fetch Data ---------
def fetch_sale_orders(uid, company_id, batch_size=1000):
    all_records, offset = [], 0
    domain = ["&", ["sales_type", "=", "oa"], ["state", "=", "sale"]]
    specification = {
        "name": {},
        "partner_id": {"fields": {"display_name": {}}},
        "company_id": {"fields": {"display_name": {}}},
        "state": {},
        "order_line": {"fields": {
            "order_id": {"fields": {"display_name": {}}},
            "product_uom_qty": {},
            "price_unit": {},
            "slidercodesfg": {},
            "price_subtotal": {},
            "product_code": {},
            "material_code": {}
        }}
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

# --------- Flatten Records ---------
def flatten_sale_order(rec):
    flat = {
        "Order Name": rec.get("name", ""),
        "Customer": safe_get(rec.get("partner_id"), "display_name"),
        "Company": safe_get(rec.get("company_id"), "display_name"),
        "State": rec.get("state", "")
    }

    order_lines = rec.get("order_line", [])
    # Flatten relational fields
    flat['Order Lines/Order Reference'] = ' / '.join([safe_get(ol.get("order_id"), "display_name") for ol in order_lines])
    flat['Order Lines/Quantity'] = ' / '.join([str(ol.get("product_uom_qty", '')) for ol in order_lines])
    flat['Order Lines/Unit Price'] = ' / '.join([str(ol.get("price_unit", '')) for ol in order_lines])
    flat['Order Lines/Slider Code (SFG)'] = ' / '.join([str(ol.get("slidercodesfg", '')) for ol in order_lines])
    flat['Order Lines/Subtotal'] = ' / '.join([str(ol.get("price_subtotal", '')) for ol in order_lines])
    flat['Order Lines/Product Code'] = ' / '.join([str(ol.get("product_code", '')) for ol in order_lines])
    flat['Order Lines/Material Code'] = ' / '.join([str(ol.get("material_code", '')) for ol in order_lines])

    return flat

# --------- Upload to Google Sheet ---------
def paste_to_gsheet(df, sheet_name):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
    if df.empty:
        print(f"Skip: {sheet_name} DataFrame is empty, not pasting.")
        return

    # Clear previous data in the range A:G
    worksheet.batch_clear(["A:G"])

    # Paste the dataframe
    set_with_dataframe(worksheet, df)

    # Add timestamp to G1
    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("G1", [[local_time]])

    print(f"✅ Data pasted to Google Sheet ({sheet_name}). Timestamp updated to G1: {local_time}")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    company_map = [(1, "OA_ITEM_DF_ZIP"), (3, "OA_ITEM_DF_MT")]

    for company_id, sheet_tab in company_map:
        records = fetch_sale_orders(uid, company_id)
        flat_records = [flatten_sale_order(r) for r in records]
        df = pd.DataFrame(flat_records)
        paste_to_gsheet(df, sheet_tab)
