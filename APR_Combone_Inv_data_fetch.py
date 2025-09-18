import os
import json
import base64
import requests
import pandas as pd
import gspread
import pytz
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# --------- Environment Variables ---------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1V0x5_DJn6bC1xzyMeBglzSeH-eDIWtKG4E5Cv3rwA_I")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "04_CI_DF")

# --------- Setup Google Credentials ---------
creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64))
creds = Credentials.from_service_account_info(
    creds_json,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


# --------- Login to Odoo ---------
def odoo_login():
    url = f"{ODOO_URL}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD},
        "id": 1,
    }
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    uid = resp.json()["result"]["uid"]
    print(f"✅ Logged in! UID: {uid}")
    return uid


# --------- Helper for safely extracting string values ---------
def get_string_value(field, subfield=None):
    if isinstance(field, dict):
        if subfield:
            value = field.get(subfield)
            return get_string_value(value)
        if "display_name" in field:
            return str(field["display_name"] or "")
        return " ".join([str(v) for v in field.values()])
    elif isinstance(field, int):
        return str(field)
    elif field in (False, None):
        return ""
    return str(field)


# --------- Fetch Combine Invoice Lines ---------
def fetch_invoice_lines(uid, start_date="2025-04-01", end_date="2025-04-30", batch_size=2000):
    all_records = []
    offset = 0

    domain = ["&", ["parent_state", "=", "posted"], "&", "&",
              ["invoice_date", ">=", start_date],
              ["invoice_date", "<=", end_date],
              ["parent_state", "=", "posted"]]

    specification = {
        "sale_order_line": {"fields": {"order_id": {"fields": {"display_name": {}}}}},
        "invoice_id": {"fields": {"display_name": {}, "lc_no": {}, "lc_date": {}, "invoice_payment_term_id": {}}},
        "buying_house": {"fields": {"display_name": {}}},
        "product_uom_category_id": {"fields": {"display_name": {}}},
        "company_id": {"fields": {"display_name": {}}},
        "invoice_date": {},
        "parent_state": {},
        "quantity": {},
        "price_total": {},
        "fg_categ_type": {},
        "sales_ots_line": {"fields": {"id": {}}},
        "marketing_ots_line": {"fields": {"id": {}}},
        "buyer_id": {"fields": {"display_name": {}}},
        "buyer_group": {"fields": {"display_name": {}}},
        "customer_id": {"fields": {"display_name": {}}},
        "customer_group": {"fields": {"display_name": {}}},
        "sales_person": {"fields": {"display_name": {}}},
        "team_id": {"fields": {"display_name": {}}},
        "country_id": {"fields": {"display_name": {}}},
    }

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "combine.invoice.line",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "domain": domain,
                    "specification": specification,
                    "offset": offset,
                    "limit": batch_size,
                    "order": "",
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": uid,
                        "allowed_company_ids": [1, 3],
                        "bin_size": True,
                        "current_company_id": 1,
                    },
                    "count_limit": 100000,
                },
            },
            "id": 3,
        }
        resp = session.post(f"{ODOO_URL}/web/dataset/call_kw/combine.invoice.line/web_search_read",
                            data=json.dumps(payload))
        resp.raise_for_status()
        result = resp.json()["result"]
        records = result.get("records", [])
        all_records.extend(records)
        print(f"Fetched {len(records)} records, total so far: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size

    print(f"✅ Finished. Total fetched: {len(all_records)}")
    return all_records


# --------- Flatten Records ---------
def flatten_invoice_records(records):
    return [{
        "Sale Order Ref": get_string_value(r.get("sale_order_line"), "order_id"),
        "Customer Invoice Items": get_string_value(r.get("invoice_id")),
        "Buying House": get_string_value(r.get("buying_house")),
        "Category": get_string_value(r.get("product_uom_category_id")),
        "Company": get_string_value(r.get("company_id")),
        "Invoice Date": get_string_value(r.get("invoice_date")),
        "Status": get_string_value(r.get("parent_state")),
        "Quantity": r.get("quantity", 0),
        "Total": r.get("price_total", 0),
        "Item": get_string_value(r.get("fg_categ_type")),
        "Sales Ots Line ID": get_string_value(r.get("sales_ots_line"), "id"),
        "Marketing Ots Line ID": get_string_value(r.get("marketing_ots_line"), "id"),
        "LC No": get_string_value(r.get("invoice_id"), "lc_no"),
        "LC Date": get_string_value(r.get("invoice_id"), "lc_date"),
        "Payment Terms": get_string_value(r.get("invoice_id"), "invoice_payment_term_id"),
        "Buyer": get_string_value(r.get("buyer_id")),
        "Buyer Group": get_string_value(r.get("buyer_group")),
        "Customer": get_string_value(r.get("customer_id")),
        "Customer Group": get_string_value(r.get("customer_group")),
        "Sales Person": get_string_value(r.get("sales_person")),
        "Team": get_string_value(r.get("team_id")),
        "Country": get_string_value(r.get("country_id")),
    } for r in records]


# --------- Normalize Dates & Group ---------
def normalize_dates_and_group(df: pd.DataFrame):
    # Convert datetime64[ns] columns to just date (drop time component)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    # Separate numeric and non-numeric columns
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    group_cols = [c for c in df.columns if c not in numeric_cols]

    # Group and sum numeric columns
    return df.groupby(group_cols, dropna=False)[numeric_cols].sum().reset_index()



# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_TAB_NAME)
    if df.empty:
        print(f"⚠️ Skip: {SHEET_TAB_NAME} DataFrame is empty.")
        return
    worksheet.batch_clear(["A:V"])
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("W1", [[f"Last Updated: {local_time}"]])
    print(f"✅ Data pasted to Google Sheet ({SHEET_TAB_NAME}), timestamp: {local_time}")


if __name__ == "__main__":
    uid = odoo_login()
    records = fetch_invoice_lines(uid)
    flat_rows = flatten_invoice_records(records)
    df = pd.DataFrame(flat_rows)
    grouped_df = normalize_dates_and_group(df)
    paste_to_gsheet(grouped_df)
