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
        "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD},
        "id": 1
    }
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    result = resp.json().get("result")
    if not result or "uid" not in result:
        raise Exception("❌ Odoo login failed")
    print(f"✅ Logged in! UID: {result['uid']}")
    return result["uid"]

# --------- Fetch Data ---------
def fetch_sale_orders(uid, company_id, batch_size=1000):
    all_records, offset = [], 0
    domain = ["&", ["sales_type", "=", "oa"], ["state", "=", "sale"]]

    specification = {
        "order_line": {"fields": {
            "order_id": {"fields": {
                "name": {},
                "order_ref": {},
                "buyer_name": {"fields": {"display_name": {}, "brand": {}}},
                "buying_house": {"fields": {"display_name": {}}},
                "company_id": {"fields": {"display_name": {}}},
                "partner_id": {"fields": {"display_name": {}, "group": {}}},
                "date_order": {},
                "team_id": {"fields": {"display_name": {}}},
                "user_id": {"fields": {"display_name": {}}},
                "lc_number": {},
                "payment_term_id": {"fields": {"display_name": {}}},
                "state": {}
            }},
            "product_template_id": {"fields": {"fg_categ_type": {}}},
            "product_uom_qty": {},
            "price_total": {},
            "slidercodesfg": {}
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
        result = resp.json()["result"]
        records = result["records"]
        all_records.extend(records)
        print(f"[Company {company_id}] Fetched {len(records)} records (Total: {len(all_records)})")
        if len(records) < batch_size:
            break
        offset += batch_size

    return all_records

# --------- Flatten ---------
def safe_get(obj, *keys):
    """Safely extract nested dict keys."""
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return ""
    return obj if obj not in (False, None) else ""

def flatten_sale_order(record):
    flat_records = []
    for line in record["order_line"]:
        order = line.get("order_id", {})
        buyer = safe_get(order, "buyer_name")
        customer = safe_get(order, "partner_id")
        flat_records.append({
            "Order Lines/Order Reference": safe_get(order, "name"),
            "Order Lines/Order Reference/Sales Order Ref.": safe_get(order, "order_ref"),
            "Order Lines/Order Reference/Buyer": buyer[1] if isinstance(buyer, (list, tuple)) else buyer,
            "Order Lines/Order Reference/Buyer/Brand Group": safe_get(buyer, "brand"),
            "Order Lines/Order Reference/Buying House": safe_get(order, "buying_house", "display_name"),
            "Order Lines/Order Reference/Company": safe_get(order, "company_id", "display_name"),
            "Order Lines/Order Reference/Customer": customer[1] if isinstance(customer, (list, tuple)) else customer,
            "Order Lines/Order Reference/Customer/Group": safe_get(customer, "group"),
            "Order Lines/Order Reference/Order Date": safe_get(order, "date_order"),
            "Order Lines/Order Reference/Sales Team": safe_get(order, "team_id", "display_name"),
            "Order Lines/Order Reference/Salesperson": safe_get(order, "user_id", "display_name"),
            "Order Lines/Order Reference/LC Number": safe_get(order, "lc_number"),
            "Order Lines/Order Reference/Payment Terms": safe_get(order, "payment_term_id", "display_name"),
            "Order Lines/Order Reference/Status": safe_get(order, "state"),
            "Order Lines/Product Template/FG Category": safe_get(line, "product_template_id", "fg_categ_type"),
            "Order Lines/Quantity": line.get("product_uom_qty"),
            "Order Lines/Total": line.get("price_total"),
            "Order Lines/Slider Code (SFG)": line.get("slidercodesfg"),
        })
    return flat_records

# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df, sheet_name):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
    if df.empty:
        print(f"⚠️ Skip: {sheet_name} is empty")
        return
    worksheet.clear()
    set_with_dataframe(worksheet, df)
    worksheet.update("A1", [["Last Updated", datetime.now(pytz.timezone("Asia/Dhaka")).strftime("%Y-%m-%d %H:%M:%S")]])
    print(f"✅ Data pasted to {sheet_name}")

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    company_map = [(1, "OA_ITEM_DF_ZIP"), (3, "OA_ITEM_DF_MT")]

    for company_id, sheet_tab in company_map:
        records = fetch_sale_orders(uid, company_id)
        all_flat_records = []
        for r in records:
            all_flat_records.extend(flatten_sale_order(r))

        df = pd.DataFrame(all_flat_records)

        # Automatically find numeric columns for aggregation
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

        # Automatically find non-numeric columns for group by
        group_cols = [col for col in df.columns if col not in numeric_cols]

        # Create aggregation dictionary dynamically (sum for numbers)
        agg_dict = {col: "sum" for col in numeric_cols}

        # Group by ALL non-numeric columns
        df_grouped = df.groupby(group_cols, as_index=False).agg(agg_dict)

        paste_to_gsheet(df_grouped, sheet_tab)