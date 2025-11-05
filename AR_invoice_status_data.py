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
from dotenv import load_dotenv
load_dotenv()
# --------- Environment Variables ---------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1sPVTbTppdEn7_S2hFyYGTF2pUoyOx19NM4siqbCKFCw")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Invoice Status_DF")  # change tab name if needed

# --------- Setup Google Credentials ---------
creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64))
creds = Credentials.from_service_account_info(
    creds_json,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

session = requests.Session()
session.headers.update({"Content-Type": "application/json"})


# --------- Odoo Login ---------
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


# --------- Helper ---------
def get_string_value(field, subfield=None):
    if isinstance(field, dict):
        if subfield:
            return get_string_value(field.get(subfield))
        if "display_name" in field:
            return str(field["display_name"] or "")
        return " ".join([str(v) for v in field.values()])
    elif isinstance(field, int):
        return str(field)
    elif field in (False, None):
        return ""
    return str(field)


# --------- Fetch combine.invoice ---------
def fetch_combine_invoice(uid, batch_size=2000):
    all_records = []
    offset = 0

    # Odoo search domain — empty to fetch all
    domain = []

    # Specification based on your 'namelist'
    specification = {
        "name": {},
        "acceptance_date": {},
        "acceptance_status": {},
        "finance_team_submitted_date": {},
        "commercial_handover_date": {},
        "delivery_date": {},
        "commercial_doc_revd_date": {},
        "docs_state": {},
        "invoice_status": {"fields": {"display_name": {}}},
        "oa_state": {},
        "partner_id": {"fields": {"display_name": {}}},
        "payment_maturity_date": {},
        "payment_maturity_status": {},
        "payment_recv_date": {},
        "tentative_acceptance_date": {},
        "tentative_payment_maturity_date": {},
        "amount_total": {},
        "due_amt": {},
        "total_recv_amt":{}
    }

    while True:
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
                        "allowed_company_ids": [1, 3, 2, 4],
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

        print(f"Fetched {len(records)} records, total: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size

    print(f"✅ Done. Total fetched: {len(all_records)}")
    return all_records


# --------- Flatten Records ---------
def flatten_invoice_summary(records):
    return [{
        "Number": get_string_value(r.get("name")),
        "Partner": get_string_value(r.get("partner_id")),
        "Delivery Date": get_string_value(r.get("delivery_date")),
        "Doc Received Date": get_string_value(r.get("commercial_doc_revd_date")),
        "Handover Date": get_string_value(r.get("commercial_handover_date")),
        "Bank Submission Date": get_string_value(r.get("finance_team_submitted_date")),
        "Acceptance Status": get_string_value(r.get("acceptance_status")),
        "Acceptance Date": get_string_value(r.get("acceptance_date")),
        "Tentative Acceptance Date": get_string_value(r.get("tentative_acceptance_date")),
        "Payment Maturity Status": get_string_value(r.get("payment_maturity_status")),
        "Payment Maturity Date": get_string_value(r.get("payment_maturity_date")),
        "Tentative Payment Maturity Date": get_string_value(r.get("tentative_payment_maturity_date")),
        "Payment Received Date": get_string_value(r.get("payment_recv_date")),
        "OA State": get_string_value(r.get("oa_state")),
        "Invoice Status": get_string_value(r.get("invoice_status")),
        "Document State": get_string_value(r.get("docs_state")),
        "Total Value": r.get("amount_total", 0),
        "Due Amount": r.get("due_amt", 0),
        "total_recv_amt":r.get("total_recv_amt", 0)
    } for r in records]


# --------- Normalize Dates ---------
def normalize_dates(df: pd.DataFrame):
    date_cols = [c for c in df.columns if "Date" in c]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_TAB_NAME)
    if df.empty:
        print(f"⚠️ Skip: {SHEET_TAB_NAME} DataFrame is empty.")
        return
    worksheet.batch_clear(["A:S"])
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("T1", [[f"Last Updated: {local_time}"]])
    print(f"✅ Data pasted to Google Sheet ({SHEET_TAB_NAME}), timestamp: {local_time}")


# --------- MAIN ---------
if __name__ == "__main__":
    uid = odoo_login()
    records = fetch_combine_invoice(uid)
    flat_rows = flatten_invoice_summary(records)
    df = pd.DataFrame(flat_rows)
    df = normalize_dates(df)
    paste_to_gsheet(df)