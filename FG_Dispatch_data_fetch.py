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

# --------- Compute Date Range: May 1 to Previous Month End ---------
def get_date_range():
    logger.info("Computing date range...")
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
    """
    Safely extract a string from Odoo API fields.
    Handles:
      - dict with display_name or nested fields
      - int (ID)
      - str
      - False/None
    """
    if isinstance(field, dict):
        if subfield:
            value = field.get(subfield)
            return get_string_value(value)
        if "display_name" in field:
            return str(field["display_name"] or "")
        # fallback: join all dict values as string
        return " ".join([str(v) for v in field.values()])
    elif isinstance(field, int):
        return str(field)
    elif field in (False, None):
        return ""
    return str(field)

# --------- Fetch invoice dates for line IDs ---------
def fetch_invoice_dates(uid, line_ids):
    logger.info(f"Fetching invoice dates for {len(line_ids)} unique line IDs...")
    if not line_ids:
        return {}
    
    domain = [["id", "in", list(line_ids)]]
    # Assuming invoice_line_id relates to account.move.line; if it's directly to account.move, change model to "account.move" and fields to ["id", "invoice_date"]
    # and line_to_date = {rec["id"]: get_string_value(rec.get("invoice_date")) for rec in result}
    fields = ["id", "move_id.invoice_date"]
    
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "account.move.line",
            "method": "search_read",
            "args": [],
            "kwargs": {
                "domain": domain,
                "fields": fields,
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": uid,
                    "allowed_company_ids": [1, 3],
                    "bin_size": True,
                },
            },
        },
        "id": 4,
    }
    resp = session.post(
        f"{ODOO_URL}/web/dataset/call_kw/account.move.line/search_read",
        data=json.dumps(payload),
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        logger.error(f"Odoo API Error: {json.dumps(data['error'])}")
        raise ValueError(data['error']['data']['message'])
    result = data["result"]
    line_to_date = {rec["id"]: get_string_value(rec.get("move_id.invoice_date")) for rec in result}
    logger.info(f"Fetched {len(line_to_date)} invoice dates")
    return line_to_date

# --------- Fetch All Operation Details for a Specific Company ---------
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
        # Fetch only IDs for invoice_line_id
        "invoice_line_id": {},
    }

    # Optional: get total count
    logger.info(f"Getting total count for Company {company_id}...")
    count_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "operation.details",
            "method": "web_search_read",
            "args": [],
            "kwargs": {"domain": domain, "specification": {"id": {}}, "limit": 1},
        },
        "id": 99,
    }
    count_resp = session.post(
        f"{ODOO_URL}/web/dataset/call_kw/operation.details/web_search_read",
        data=json.dumps(count_payload),
    )
    count_resp.raise_for_status()
    count_data = count_resp.json()
    if "error" in count_data:
        logger.error(f"Odoo API Error (count): {json.dumps(count_data['error'])}")
        raise ValueError(count_data['error']['data']['message'])
    total_count = count_data["result"]["length"]
    logger.info(f"Total records to fetch for Company {company_id}: {total_count}")

    while True:
        logger.debug(f"Fetching batch: offset={offset}, limit={batch_size} for Company {company_id}")
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
        data = resp.json()
        if "error" in data:
            logger.error(f"Odoo API Error: {json.dumps(data['error'])}")
            raise ValueError(data['error']['data']['message'])
        result = data["result"]
        records = result.get("records", [])
        all_records.extend(records)
        logger.info(
            f"Fetched {len(records)} records for Company {company_id}, total so far: {len(all_records)}/{total_count}"
        )
        if len(records) < batch_size:
            break
        offset += batch_size

    logger.info(f"Finished fetching for Company {company_id}. Total records: {len(all_records)}")
    return all_records

# --------- Flatten Records into Rows ---------
def flatten_records(records, line_to_date):
    logger.info(f"Flattening {len(records)} records...")
    flat_rows = []
    for record in records:
        # Handle both many2one and one2many for invoice_line_id
        invoice_field = record.get("invoice_line_id", False)
        invoice_lines = []
        if invoice_field:
            if isinstance(invoice_field, list):
                if len(invoice_field) > 0:
                    if isinstance(invoice_field[0], int):
                        # one2many: list of ids
                        invoice_lines = invoice_field
                    elif len(invoice_field) == 2 and isinstance(invoice_field[0], (int, bool)) and isinstance(invoice_field[1], str):
                        # many2one: [id, display_name]
                        if invoice_field[0]:
                            invoice_lines = [invoice_field[0]]
        invoice_dates = set(line_to_date.get(lid, "") for lid in invoice_lines)
        invoice_date_str = ", ".join(d for d in sorted(invoice_dates) if d)

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
    logger.info(f"Flattened {len(flat_rows)} rows")
    return flat_rows

# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df):
    logger.info(f"Pasting {len(df)} rows to Google Sheet '{SHEET_TAB_NAME}'...")
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_TAB_NAME)
    if df.empty:
        logger.warning(f"Skip: {SHEET_TAB_NAME} DataFrame is empty.")
        return
    logger.info("Clearing existing data in range A:R...")
    worksheet.batch_clear(["A:S"])
    logger.info("Setting dataframe to worksheet...")
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Updating timestamp in S1...")
    worksheet.update("T1", [[f"Last Updated: {local_time}"]])
    logger.info(f"Data pasted to Google Sheet ({SHEET_TAB_NAME}), timestamp: {local_time}")

# --------- Main ---------
if __name__ == "__main__":
    logger.info("Starting main script...")
    uid = odoo_login()
    
    # Fetch for both companies
    companies = [1, 3]
    all_records = []
    unique_line_ids = set()
    
    for company_id in companies:
        logger.info(f"Starting fetch for Company {company_id}...")
        records = fetch_operation_details(uid, company_id)
        all_records.extend(records)
        for record in records:
            invoice_field = record.get("invoice_line_id", False)
            if invoice_field and isinstance(invoice_field, list):
                if len(invoice_field) > 0:
                    if isinstance(invoice_field[0], int):
                        # one2many: list of ids
                        for lid in invoice_field:
                            if lid:
                                unique_line_ids.add(lid)
                    elif len(invoice_field) == 2 and isinstance(invoice_field[0], (int, bool)) and isinstance(invoice_field[1], str):
                        # many2one: [id, display_name]
                        if invoice_field[0]:
                            unique_line_ids.add(invoice_field[0])
    
    logger.info(f"Unique line IDs collected: {len(unique_line_ids)}")
    
    # Fetch dates
    line_to_date = fetch_invoice_dates(uid, unique_line_ids)
    
    # Now flatten
    all_flat_rows = flatten_records(all_records, line_to_date)
    
    logger.info(f"Combining data from all companies: {len(all_flat_rows)} total rows")
    df = pd.DataFrame(all_flat_rows)
    
    # Create Value column: Final Price * Qty
    logger.info("Creating Value column (Final Price * Qty)...")
    df['Value'] = df['Final Price'] * df['Qty']
    
    # Convert date columns to date only (discard time)
    logger.info("Converting date columns to date only...")
    date_cols = ['Action Date', 'Order Date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date.astype(str)
    
    # Group by all columns except aggregation columns
    logger.info("Performing groupby aggregation...")
    agg_columns = ['FG Balance', 'Qty', 'Final Price', 'Value']
    group_columns = [col for col in df.columns if col not in agg_columns]
    agg_dict = {
        'FG Balance': 'sum',
        'Qty': 'sum',
        'Final Price': 'mean',
        'Value': 'sum'
    }
    
    df_grouped = df.groupby(group_columns).agg(agg_dict).reset_index()
    logger.info(f"Grouped data: {len(df_grouped)} rows, {len(df_grouped.columns)} columns")
    
    # Save to Excel (optional, for local runs)
    logger.info("Saving to Excel file...")
    df_grouped.to_excel("operation_details_grouped.xlsx", index=False)
    logger.info(f"Excel saved with {len(df_grouped)} rows and {len(df_grouped.columns)} columns.")
    
    # Paste to Google Sheet
    paste_to_gsheet(df_grouped)
    
    logger.info("Script completed successfully.")