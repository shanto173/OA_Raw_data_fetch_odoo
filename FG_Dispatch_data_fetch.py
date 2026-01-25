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

# --------- Configuration (expect via env) ---------
ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1V0x5_DJn6bC1xzyMeBglzSeH-eDIWtKG4E5Cv3rwA_I")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "FG_DSP_DF")

# --------- Setup Google Credentials (if provided) ---------
if GOOGLE_CREDENTIALS_BASE64:
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64))
    creds = Credentials.from_service_account_info(
        creds_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
else:
    gc = None
    logger.warning("GOOGLE_CREDENTIALS_BASE64 not set; Google Sheets functionality will be skipped.")

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
    resp_json = resp.json()
    if "error" in resp_json:
        logger.error("Login error: %s", resp_json["error"])
        raise ValueError(resp_json["error"])
    uid = resp_json["result"]["uid"]
    logger.info(f"Login successful, UID: {uid}")
    return uid

# --------- Compute Date Range: May 1 to Previous Month End ---------
def get_date_range():
    logger.info("Computing date range...")
    today = datetime.now()
    current_year = today.year
    
    # Keep start date as May 1st of current year
    start_date = f"{current_year}-05-01 00:00:00"
    
    # Use today as end date (including current time)
    end_date = today.strftime("%Y-%m-%d %H:%M:%S")
    
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
      - list forms like [id, name]
    """
    if isinstance(field, dict):
        if subfield:
            value = field.get(subfield)
            return get_string_value(value)
        if "display_name" in field:
            return str(field["display_name"] or "")
        # fallback: join all dict values as string
        return " ".join([str(v) for v in field.values() if v is not None])
    elif isinstance(field, list):
        # often like [id, name]
        if len(field) >= 2:
            return str(field[1] or "")
        if len(field) == 1:
            return str(field[0])
        return ""
    elif isinstance(field, int):
        return str(field)
    elif field in (False, None):
        return ""
    return str(field)

# --------- Fetch invoice dates and statuses for line IDs (fallback) ---------
def fetch_invoice_data(uid, line_ids):
    """
    Given a set/list of combine.invoice.line IDs, fetch invoice_date and parent_state for them.
    Uses web_search_read with specification to get invoice_date and parent_state.
    Returns dict: {line_id: {"date": invoice_date_str, "status": parent_state_str}}
    """
    logger.info(f"Fetching invoice data for {len(line_ids)} unique line IDs (fallback)...")
    if not line_ids:
        return {}

    domain = [["id", "in", list(line_ids)]]

    # Request invoice_date and parent_state via specification
    specification = {
        "id": {},
        "invoice_date": {},
        "parent_state": {},
    }

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
                "limit": len(line_ids),
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                },
            },
        },
        "id": 9999,
    }

    resp = session.post(f"{ODOO_URL}/web/dataset/call_kw/combine.invoice.line/web_search_read", data=json.dumps(payload))
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        logger.error(f"Odoo API Error (fetch_invoice_data): {json.dumps(data['error'])}")
        return {}

    records = data.get("result", {}).get("records", [])
    line_to_data = {}
    for rec in records:
        lid = rec.get("id")
        invoice_date = rec.get("invoice_date", "") or ""
        invoice_status = rec.get("parent_state", "") or ""
        # If data exists, store
        if lid and (invoice_date or invoice_status):
            line_to_data[lid] = {"date": invoice_date, "status": invoice_status}
    logger.info(f"Fetched {len(line_to_data)} invoice data entries via fallback")
    return line_to_data

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

    # --- Specification with direct fields on invoice_line_id ---
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
        # Direct fields for invoice_line_id
        "invoice_line_id": {
            "fields": {
                "id": {},  # keep id for fallback mapping if needed
                "invoice_date": {},   # ✅ direct on combine.invoice.line
                "parent_state": {},   # ✅ direct on combine.invoice.line
             }
        },
    }

    # Optional: get total count (uses minimal specification to get length)
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
    total_count = count_data["result"].get("length", 0)
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

# --------- Flatten Records into Rows (robust invoice date and status extraction) ---------
def flatten_records(records, line_to_data_fallback):
    logger.info(f"Flattening {len(records)} records...")
    flat_rows = []
    for record in records:
        invoice_field = record.get("invoice_line_id", False)
        invoice_line_ids_for_fallback = set()
        invoice_dates = set()
        invoice_statuses = set()

        if not invoice_field:
            pass
        elif isinstance(invoice_field, dict):
            # Case: many2one with fields
            inv_date = invoice_field.get("invoice_date", "")
            inv_status = invoice_field.get("parent_state", "")
            if inv_date:
                invoice_dates.add(str(inv_date))
            if inv_status:
                invoice_statuses.add(str(inv_status))

            lid = invoice_field.get("id")
            if lid and (not inv_date or not inv_status):
                invoice_line_ids_for_fallback.add(lid)
        elif isinstance(invoice_field, list):
            if invoice_field:
                if isinstance(invoice_field[0], dict):
                    # Case A: list of dicts for x2many
                    for entry in invoice_field:
                        inv_date = entry.get("invoice_date", "")
                        inv_status = entry.get("parent_state", "")
                        if inv_date:
                            invoice_dates.add(str(inv_date))
                        if inv_status:
                            invoice_statuses.add(str(inv_status))

                        lid = entry.get("id")
                        if lid and (not inv_date or not inv_status):
                            invoice_line_ids_for_fallback.add(lid)
                else:
                    # Fallback: list of ints or [id, display]
                    if isinstance(invoice_field[0], int):
                        for lid in invoice_field:
                            invoice_line_ids_for_fallback.add(lid)
                    elif isinstance(invoice_field[0], (list, tuple)):
                        for el in invoice_field:
                            if len(el) >= 1 and isinstance(el[0], int):
                                invoice_line_ids_for_fallback.add(el[0])
                    else:
                        # Unknown shape, try parse
                        for el in invoice_field:
                            try:
                                invoice_line_ids_for_fallback.add(int(el))
                            except Exception:
                                pass
            # Case B: many2one without fields like [id, display]
            elif len(invoice_field) == 2 and isinstance(invoice_field[0], int):
                invoice_line_ids_for_fallback.add(invoice_field[0])
        elif isinstance(invoice_field, int):
            # Unlikely, but direct id
            invoice_line_ids_for_fallback.add(invoice_field)

        # Resolve fallback invoice data from mapping
        for lid in invoice_line_ids_for_fallback:
            data = line_to_data_fallback.get(lid, {})
            d = data.get("date", "")
            s = data.get("status", "")
            if d:
                invoice_dates.add(d)
            if s:
                invoice_statuses.add(s)

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
            "Invoice Date": ", ".join(sorted(invoice_dates)),
            "Invoice Status": ", ".join(sorted(invoice_statuses)),
        })
    logger.info(f"Flattened {len(flat_rows)} rows")
    return flat_rows

# --------- Paste to Google Sheet ---------
def paste_to_gsheet(df):
    if not gc:
        logger.warning("Google client not initialized; skipping paste to Google Sheet.")
        return
    logger.info(f"Pasting {len(df)} rows to Google Sheet '{SHEET_TAB_NAME}'...")
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_TAB_NAME)
    if df.empty:
        logger.warning(f"Skip: {SHEET_TAB_NAME} DataFrame is empty.")
        return
    logger.info("Clearing existing data in range A:R...")
    worksheet.batch_clear(["A:T"])
    logger.info("Setting dataframe to worksheet...")
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Updating timestamp in T1...")
    worksheet.update("U1", [[f"Last Updated: {local_time}"]])
    logger.info(f"Data pasted to Google Sheet ({SHEET_TAB_NAME}), timestamp: {local_time}")

# --------- Main ---------
if __name__ == "__main__":
    MAX_RETRIES = 2  # Total retries for the full pipeline
    RETRY_DELAY = 20  # Seconds to wait before retry
    retries = 0

    while retries < MAX_RETRIES:
        try:
            logger.info("Starting main script attempt %d...", retries + 1)
            uid = odoo_login()

            # Fetch for both companies
            companies = [1, 3]
            all_records = []
            unique_line_ids_for_fallback = set()

            for company_id in companies:
                logger.info(f"Starting fetch for Company {company_id}...")
                records = fetch_operation_details(uid, company_id)
                all_records.extend(records)

                # collect fallback invoice line ids for lines that didn't include nested data
                for record in records:
                    invoice_field = record.get("invoice_line_id", False)
                    if not invoice_field:
                        continue
                    if isinstance(invoice_field, dict):
                        has_date = bool(invoice_field.get("invoice_date"))
                        has_status = bool(invoice_field.get("parent_state"))
                        if not has_date or not has_status:
                            lid = invoice_field.get("id")
                            if lid:
                                unique_line_ids_for_fallback.add(lid)
                    elif isinstance(invoice_field, list):
                        if invoice_field:
                            if isinstance(invoice_field[0], dict):
                                for entry in invoice_field:
                                    has_date = bool(entry.get("invoice_date"))
                                    has_status = bool(entry.get("parent_state"))
                                    if not has_date or not has_status:
                                        lid = entry.get("id")
                                        if lid:
                                            unique_line_ids_for_fallback.add(lid)
                            else:
                                if isinstance(invoice_field[0], int):
                                    for lid in invoice_field:
                                        if lid:
                                            unique_line_ids_for_fallback.add(lid)
                                elif isinstance(invoice_field[0], (list, tuple)):
                                    for el in invoice_field:
                                        if isinstance(el, (list, tuple)) and len(el) >= 1:
                                            lid = el[0]
                                            if lid:
                                                unique_line_ids_for_fallback.add(lid)
                        if len(invoice_field) == 2 and isinstance(invoice_field[0], (int, bool)):
                            lid = invoice_field[0]
                            if lid:
                                unique_line_ids_for_fallback.add(lid)

            logger.info(f"Unique invoice line IDs to fallback-fetch: {len(unique_line_ids_for_fallback)}")

            # Fetch data for the fallback IDs
            line_to_data = fetch_invoice_data(uid, unique_line_ids_for_fallback)

            # Flatten and combine
            all_flat_rows = flatten_records(all_records, line_to_data)
            logger.info(f"Combining data from all companies: {len(all_flat_rows)} total rows")
            df = pd.DataFrame(all_flat_rows)

            if not df.empty:
                # Create Value column
                df['Value'] = df['Final Price'] * df['Qty']

                # Convert date columns to date only
                date_cols = ['Action Date', 'Order Date']
                for col in date_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date.astype(str)

                # Group and aggregate
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

                # Save Excel (optional)
                df_grouped.to_excel("operation_details_grouped.xlsx", index=False)
                logger.info("Excel saved successfully.")

                # Paste to Google Sheet
                paste_to_gsheet(df_grouped)
            else:
                logger.info("No data to process; skipping grouping, export, and sheet update.")

            logger.info("Script completed successfully.")
            break  # Success, exit retry loop

        except Exception as e:
            retries += 1
            logger.error(f"Attempt {retries}/{MAX_RETRIES} failed: {e}")
            if retries < MAX_RETRIES:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                import time; time.sleep(RETRY_DELAY)
            else:
                logger.critical(f"All {MAX_RETRIES} attempts failed. Exiting.")
                raise
