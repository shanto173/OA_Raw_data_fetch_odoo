#!/usr/bin/env python3
"""
Export script: fetch sale.order for company 1 and 3 using updated domain:
  sales_type='oa' AND state='sale' AND team_id IN [17,16] AND
  date_order >= '2025-05-01 05:07:48' AND date_order <= TODAY_at_05:07:48 (Asia/Dhaka)

Pastes into Google Sheets:
 - Company 1 -> worksheet "Export Overseas OA Data" (range A:P cleared)
 - Company 3 -> worksheet "MT_Export Overseas OA Data" (range A:P cleared)

Required environment vars:
  ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, GOOGLE_CREDENTIALS_BASE64
Optional:
  GOOGLE_SHEET_ID (falls back to a built-in id)
"""
import os
import json
import base64
import requests
import pandas as pd
from datetime import datetime, time
import pytz
import traceback
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
load_dotenv()
# ---------- Config ----------
GOOGLE_SHEET_ID_FALLBACK = "1l2xcuZVCgj3yVVKerFE9iCIK1SvyHUMWpZQ5af5wbLM"

ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID =GOOGLE_SHEET_ID_FALLBACK

required = {
    "ODOO_URL": ODOO_URL,
    "ODOO_DB": ODOO_DB,
    "ODOO_USERNAME": ODOO_USERNAME,
    "ODOO_PASSWORD": ODOO_PASSWORD,
    "GOOGLE_CREDENTIALS_BASE64": GOOGLE_CREDENTIALS_BASE64
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise SystemExit(f"Missing environment variables: {missing}")

# ---------- Google client ----------
creds_b64 = GOOGLE_CREDENTIALS_BASE64.strip()
try:
    creds_raw = base64.b64decode(creds_b64)
    creds_json = json.loads(creds_raw)
except Exception:
    try:
        creds_json = json.loads(GOOGLE_CREDENTIALS_BASE64)
    except Exception as e:
        raise RuntimeError("Failed to decode GOOGLE_CREDENTIALS_BASE64: " + str(e))

scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
google_creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
gc = gspread.authorize(google_creds)

# ---------- HTTP session ----------
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

# ---------- Helpers ----------
def odoo_authenticate():
    url = f"{ODOO_URL.rstrip('/')}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD},
        "id": 1
    }
    r = session.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    uid = data.get("result", {}).get("uid")
    if not uid:
        raise RuntimeError("Failed to authenticate to Odoo. Response: " + str(data))
    print(f"Authenticated to Odoo as uid={uid}")
    return uid

# Columns specification
SPECIFICATION = {
    "amount_invoiced": {},
    "buyer_name": {},
    "partner_id": {"fields": {"display_name": {}}},
    "name": {},
    "order_ref": {"fields": {"display_name": {}}},
    "user_id": {"fields": {"display_name": {}}},
    "pi_date": {},
    "date_order": {},
    "amount_total": {},
    "total_product_qty": {}
}

def safe_display_name(obj):
    if isinstance(obj, dict):
        return obj.get("display_name", "")
    return ""

def flatten_sale_record(rec):
    return {
        "Already invoiced": rec.get("amount_invoiced", ""),
        "Buyer": rec.get("buyer_name", ""),
        "Customer": safe_display_name(rec.get("partner_id")),
        "Order Reference": rec.get("name", ""),
        "Sales Order Ref.": safe_display_name(rec.get("order_ref")),
        "Salesperson": safe_display_name(rec.get("user_id")),
        "PI Date": rec.get("pi_date", ""),
        "Order Date": rec.get("date_order", ""),
        "Total": rec.get("amount_total", ""),
        "Total PI Quantity": rec.get("total_product_qty", "")
    }

def get_date_range_strings():
    start_str = "2025-05-01 05:07:48"
    tz = pytz.timezone("Asia/Dhaka")
    now_local = datetime.now(tz)
    end_date = now_local.date()
    end_dt = datetime.combine(end_date, time(hour=5, minute=7, second=48))
    if end_dt.tzinfo is None:
        end_dt = tz.localize(end_dt)
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    return start_str, end_str

def build_odoo_domain(start_str, end_str, team_list):
    """
    Build domain in the exact prefixed-& format expected by Odoo UI logs.
    team_list should be a Python list, e.g. [17, 16]
    """
    return [
        "&", "&", "&", "&",
        ["sales_type", "=", "oa"],
        ["state", "=", "sale"],
        ["team_id", "in", team_list],
        ["date_order", ">=", start_str],
        ["date_order", "<=", end_str]
    ]

# ---------- Fetching (paginated) ----------
def fetch_sale_orders(uid, company_id, team_list=[17, 16], batch_size=1000):
    endpoint = f"{ODOO_URL.rstrip('/')}/web/dataset/call_kw/sale.order/web_search_read"
    offset = 0
    all_records = []

    start_str, end_str = get_date_range_strings()
    domain = build_odoo_domain(start_str, end_str, team_list)

    print("DEBUG: Using domain:")
    print(json.dumps(domain, indent=2))

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "sale.order",
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": SPECIFICATION,
                    "domain": domain,
                    "offset": offset,
                    "limit": batch_size,
                    "order": "",
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
            "id": 200 + offset
        }
        try:
            resp = session.post(endpoint, json=payload, timeout=60)
            resp.raise_for_status()
            body = resp.json()
        except Exception as e:
            print("Error calling web_search_read:", e)
            traceback.print_exc()
            raise

        records = body.get("result", {}).get("records", [])
        fetched = len(records)
        print(f"[company {company_id}] fetched {fetched} rows (offset={offset})")
        all_records.extend(records)
        if fetched < batch_size:
            break
        offset += batch_size

    print(f"[company {company_id}] total records fetched: {len(all_records)} (date_range: {start_str} -> {end_str})")
    return all_records

# ---------- Paste to sheet (A:P) ----------
def paste_dataframe_to_sheet(df, worksheet_name):
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows="100", cols="20")
    ws.batch_clear(["A:P"])
    if not df.empty:
        set_with_dataframe(ws, df)  # writes starting at A1
        print(f"Pasted {len(df)} rows to '{worksheet_name}'.")
    else:
        print(f"No rows to paste for '{worksheet_name}'.")
    tz = pytz.timezone("Asia/Dhaka")
    ts = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    ws.update(range_name="P1", values=[[ts]])
    print(f"Timestamp written to P1: {ts}")

# ---------- Main ----------
def main():
    uid = odoo_authenticate()

    company_map = [
        (1, "Export Overseas OA Data"),
        (3, "MT_Export Overseas OA Data")
    ]

    for cid, sheet_name in company_map:
        try:
            records = fetch_sale_orders(uid, cid, team_list=[17, 16], batch_size=500)
            flat = [flatten_sale_record(r) for r in records]
            df = pd.DataFrame(flat, columns=[
                "Already invoiced", "Buyer", "Customer", "Order Reference",
                "Sales Order Ref.", "Salesperson", "PI Date", "Order Date",
                "Total", "Total PI Quantity"
            ])
            paste_dataframe_to_sheet(df, sheet_name)
        except Exception as e:
            print(f"Failed for company {cid} -> sheet {sheet_name}: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    main()
