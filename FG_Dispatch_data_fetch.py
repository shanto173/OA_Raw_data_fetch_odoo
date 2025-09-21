import requests
import json
import pandas as pd
from datetime import datetime
import calendar
import os
import base64
import gspread
from google.oauth2.service_account import Credentials
import pytz
from gspread_dataframe import set_with_dataframe
import logging

# --------- Setup Logging ---------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --------- Configuration ---------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1V0x5_DJn6bC1xzyMeBglzSeH-eDIWtKG4E5Cv3rwA_I")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "FG_DSP_DF")

# --------- Setup Google Credentials ---------
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
    logger.info("Starting Odoo login...")
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
    logger.info(f"Login successful, UID: {uid}")
    return uid

# --------- Compute Date Range ---------
def get_date_range():
    today = datetime.now()
    current_year = today.year
    start_date = f"{current_year}-05-01 00:00:00"

    current_month = today.month
    if current_month == 1:
        prev_year = current_year - 1
        prev_month = 12
    else:
        prev_year = current_year
        prev_month = current_month - 1
    _, last_day = calendar.monthrange(prev_year, prev_month)
    end_date = f"{prev_year}-{prev_month:02d}-{last_day:02d} 23:59:59"
    logger.info(f"Date range computed: {start_date} to {end_date}")
    return start_date, end_date

# --------- Helper to safely get string values ---------
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

# --------- Fetch All Operation Details ---------
def fetch_operation_details(uid, company_id, batch_size=5000):
    logger.info(f"Starting fetch for Company {company_id}...")
    start_date, end_date = get_date_range()

    all_records = []
    offset = 0

    domain = [
        "&",
        ["next_operation", "=", "Delivery"],
        "&",
        ["state", "!=", "done"],
        ["state", "!=", "closed"],
        "&",
        ["action_date", ">=", start_date],
        ["action_date", "<=", end_date],
        ["company_id", "=", company_id]
    ]

    # Add invoice_line_id -> move_id -> invoice_date
    specification = {
        "action_date": {},
        "company_id": {"fields": {"display_name": {}}},
        "fg_balance": {},
        "fg_categ_type": {"fields": {"display_name": {}}},
        "oa_id": {"fields": {"display_name": {}}},
        "date_order": {},
        "product_template_id": {"fields": {"display_name": {}}},
        "product_id": {"fields": {"display_name": {}}},
        "final_price": {},
        "qty": {},
        "team_id": {"fields": {"display_name": {}}},
        "sales_person": {"fields": {"display_name": {}}},
        "customer_group": {"fields": {"display_name": {}}},
        "partner_id": {"fields": {"display_name": {}}},
        "buyer_name": {},
        "buyer_group": {"fields": {"display_name": {}}},
        "country_id": {"fields": {"display_name": {}}},
        "invoice_line_id": {"fields": {"move_id": {"fields": {"invoice_date": {}}}}}
    }

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "operation.details",
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
                        "current_company_id": company_id,
                    },
                    "count_limit": 100000,
                },
            },
            "id": 3,
        }
        resp = session.post(
            f"{ODOO_URL}/web/dataset/call_kw/operation.details/web_search_read",
            data=json.dumps(payload),
        )
        resp.raise_for_status()
        resp_data = resp.json()
        if "result" not in resp_data:
            logger.error(f"Odoo API returned error: {resp_data}")
            break
        result = resp_data["result"]

        records = result.get("records", [])
        all_records.extend(records)
        logger.info(f"Fetched {len(records)} records, total so far: {len(all_records)}")
        if len(records) < batch_size:
            break
        offset += batch_size

    logger.info(f"Finished fetching for Company {company_id}. Total records: {len(all_records)}")
    return all_records

# --------- Flatten Records ---------
def flatten_records(records):
    flat_rows = []
    for record in records:
        # Extract invoice dates safely
        invoice_dates = []
        for inv_line in record.get("invoice_line_id", []):
            move = inv_line.get("move_id")
            if move:
                invoice_dates.append(get_string_value(move.get("invoice_date")))
        invoice_date_str = ", ".join(invoice_dates)

        flat_rows.append({
            "Action Date": get_string_value(record.get("action_date")),
            "Company": get_string_value(record.get("company_id")),
            "FG Balance": record.get("fg_balance", 0),
            "Item": get_string_value(record.get("fg_categ_type")),
            "OA": get_string_value(record.get("oa_id")),
            "Order Date": get_string_value(record.get("date_order")),
            "Product": get_string_value(record.get("product_template_id")),
            "Product Id": get_string_value(record.get("product_id")),
            "Final Price": record.get("final_price", 0),
            "Qty": record.get("qty", 0),
            "Team": get_string_value(record.get("team_id")),
            "Sales Person": get_string_value(record.get("sales_person")),
            "Customer Group": get_string_value(record.get("customer_group")),
            "Customer": get_string_value(record.get("partner_id")),
            "Buyer": get_string_value(record.get("buyer_name")),
            "Buyer Group": get_string_value(record.get("buyer_group")),
            "Country": get_string_value(record.get("country_id")),
            "Invoice Date": invoice_date_str,
        })
    return flat_rows

# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df):
    logger.info(f"Pasting {len(df)} rows to Google Sheet '{SHEET_TAB_NAME}'...")
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_TAB_NAME)
    if df.empty:
        logger.warning(f"Skip: {SHEET_TAB_NAME} DataFrame is empty.")
        return
    worksheet.batch_clear(["A:S"])
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("T1", [[f"Last Updated: {local_time}"]])

# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    companies = [1, 3]
    all_flat_rows = []

    for company_id in companies:
        records = fetch_operation_details(uid, company_id)
        flat_rows = flatten_records(records)
        all_flat_rows.extend(flat_rows)

    df = pd.DataFrame(all_flat_rows)
    df['Value'] = df['Final Price'] * df['Qty']

    # Convert date columns to date only
    date_cols = ['Action Date', 'Order Date', 'Invoice Date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date.astype(str)

    # Grouping
    agg_columns = ['FG Balance', 'Qty', 'Final Price', 'Value']
    group_columns = [col for col in df.columns if col not in agg_columns]
    agg_dict = {
        'FG Balance': 'sum',
        'Qty': 'sum',
        'Final Price': 'mean',
        'Value': 'sum'
    }
    df_grouped = df.groupby(group_columns).agg(agg_dict).reset_index()

    # Optional: Save locally
    df_grouped.to_excel("operation_details_grouped.xlsx", index=False)

    paste_to_gsheet(df_grouped)
    logger.info("Script completed successfully.")
