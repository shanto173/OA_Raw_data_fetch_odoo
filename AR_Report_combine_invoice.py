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
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1sPVTbTppdEn7_S2hFyYGTF2pUoyOx19NM4siqbCKFCw")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Raw_Data")

# --------- Setup Google Credentials ---------
print("🔹 Setting up Google credentials...")
creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64))
creds = Credentials.from_service_account_info(
    creds_json,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)
print("✅ Google credentials authorized.")

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


# --------- Login to Odoo ---------
def odoo_login():
    print("🔹 Logging into Odoo...")
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
    print(f"✅ Logged in to Odoo! UID: {uid}")
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


# --------- Fetch Combine Invoice ---------
def fetch_combine_invoice(uid, batch_size=2000):
    print("🔹 Starting fetch for combine.invoice...")
    all_records = []
    offset = 0

    # Domain: only posted invoices
    domain = [["state", "=", "posted"]]

    # Specification: only requested fields
    specification = {
        "name": {},
        "invoice_date": {},
        "buyer_name": {"fields": {"display_name": {}}},
        "partner_id": {"fields": {"display_name": {}}},
        "delivery_date": {},
        "amount_total": {},
        "commercial_doc_revd_date": {},
        "commercial_handover_date": {},
        "finance_team_submitted_date": {},
        "acceptance_date": {},
        "docs_state": {},
        "oa_state": {},
        "payment_maturity_date": {},
        "payment_recv_date": {},
        "invoice_payment_term_id": {"fields": {"display_name": {}}},
        "lc_no": {},
        "lc_date": {},
        "currency_id": {"fields": {"display_name": {}}},
        "fg_delivery": {},
        "fg_delivery_pending": {},
        "fg_receiving": {},
        "m_total": {},
        "m_total_q": {},
        "m_invoice": {},
        "pi_numbers": {},
        "production_qty": {},
        "qty_total": {},
        "production_pending": {},
        "po_numbers": {},
        "total_oa_product_qty": {},
        "z_total": {},
        "z_total_q": {},
        "z_invoice": {},
    }

    while True:
        print(f"🔹 Fetching batch starting at offset {offset}...")
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "combine.invoice",
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
            "id": 2,
        }

        resp = session.post(f"{ODOO_URL}/web/dataset/call_kw/combine.invoice/web_search_read",
                            data=json.dumps(payload))
        resp.raise_for_status()
        result = resp.json()["result"]
        records = result.get("records", [])
        all_records.extend(records)
        print(f"   ✅ Fetched {len(records)} records in this batch. Total so far: {len(all_records)}")
        if len(records) < batch_size:
            print("🔹 No more records to fetch.")
            break
        offset += batch_size

    print(f"✅ Finished fetching all records. Total: {len(all_records)}")
    return all_records


# --------- Flatten Records ---------
def flatten_invoice_records(records):
    print("🔹 Flattening records...")
    flat = [{
        "Number": get_string_value(r.get("name")),
        "Invoice/Bill Date": get_string_value(r.get("invoice_date")),
        "Buyer": get_string_value(r.get("buyer_name")),
        "Partner": get_string_value(r.get("partner_id")),
        "Delivery Date": get_string_value(r.get("delivery_date")),
        "Total Value": r.get("amount_total", 0),
        "Doc Received Date from C&F": get_string_value(r.get("commercial_doc_revd_date")),
        "Commercial to Finance Handover Date": get_string_value(r.get("commercial_handover_date")),
        "Bank Submission Date": get_string_value(r.get("finance_team_submitted_date")),
        "Acceptance Date": get_string_value(r.get("acceptance_date")),
        "Document State": get_string_value(r.get("docs_state")),
        "OA State": get_string_value(r.get("oa_state")),
        "Payment Maturity Date": get_string_value(r.get("payment_maturity_date")),
        "Payment Received Date": get_string_value(r.get("payment_recv_date")),
        "Payment Terms": get_string_value(r.get("invoice_payment_term_id")),
        "LC": get_string_value(r.get("lc_no")),
        "LC Date": get_string_value(r.get("lc_date")),
        "Currency": get_string_value(r.get("currency_id")),
        "FG Delivery": r.get("fg_delivery", 0),
        "FG Delivery Pending": r.get("fg_delivery_pending", 0),
        "FG Receiving": r.get("fg_receiving", 0),
        "Metal Total": r.get("m_total", 0),
        "Metal Total Qty": r.get("m_total_q", 0),
        "Metal Trims Invoice": get_string_value(r.get("m_invoice")),
        "PI No.": get_string_value(r.get("pi_numbers")),
        "Production Qty": r.get("production_qty", 0),
        "Qty Total": r.get("qty_total", 0),
        "Production Pending": r.get("production_pending", 0),
        "PO No.": get_string_value(r.get("po_numbers")),
        "Total Released Qty": r.get("total_oa_product_qty", 0),
        "Zipper Total": r.get("z_total", 0),
        "Zipper Total Qty": r.get("z_total_q", 0),
        "Zipper Invoice": get_string_value(r.get("z_invoice")),
    } for r in records]
    print(f"✅ Flattened {len(flat)} records.")
    return flat


# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df):
    print("🔹 Pasting data to Google Sheet...")
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_TAB_NAME)
    if df.empty:
        print(f"⚠️ Skip: {SHEET_TAB_NAME} DataFrame is empty.")
        return
    worksheet.batch_clear(["A:AJ"])
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("AK1", [[f"Last Updated: {local_time}"]])
    print(f"✅ Data pasted to Google Sheet ({SHEET_TAB_NAME}), timestamp: {local_time}")


if __name__ == "__main__":
    print("🔹 Script started...")
    uid = odoo_login()
    records = fetch_combine_invoice(uid)
    flat_rows = flatten_invoice_records(records)
    df = pd.DataFrame(flat_rows)
    paste_to_gsheet(df)
    print("✅ Script finished successfully!")
