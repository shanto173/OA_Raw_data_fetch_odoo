# sync_odoo_to_gsheets.py
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
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1V0x5_DJn6bC1xzyMeBglzSeH-eDIWtKG4E5Cv3rwA_I")

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
        "id": 3,
    }
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    uid = resp.json()["result"]["uid"]
    print(f"✅ Logged in! UID: {uid}")
    return uid


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


# --------- Fetch All Sale Orders for a Company ---------
def fetch_sale_orders_for_company(uid, company_id, batch_size=2000):
    all_records = []
    offset = 0

    domain = ["&", ["sales_type", "=", "oa"], ["state", "=", "sale"]]
    specification = {
        "order_line": {
            "fields": {
                "order_id": {
                    "fields": {
                        "name": {},
                        "order_ref": {"fields": {"display_name": {}}},
                        "buyer_name": {
                            "fields": {
                                "display_name": {},
                                "brand": {"fields": {"display_name": {}}},
                            }
                        },
                        "buying_house": {"fields": {"display_name": {}}},
                        "company_id": {"fields": {"display_name": {}}},
                        "partner_id": {
                            "fields": {
                                "display_name": {},
                                "group": {"fields": {"display_name": {}}},
                            }
                        },
                        "date_order": {},
                        "team_id": {"fields": {"display_name": {}}},
                        "user_id": {"fields": {"display_name": {}}},
                        "lc_number": {},
                        "payment_term_id": {"fields": {"display_name": {}}},
                        "state": {},
                    }
                },
                "product_template_id": {
                    "fields": {
                        "fg_categ_type": {"fields": {"display_name": {}}},
                    }
                },
                "product_uom_qty": {},
                "price_total": {},
                "slidercodesfg": {},
            }
        }
    }

    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": uid,
        "allowed_company_ids": [company_id],
        "bin_size": True,
        "current_company_id": company_id,
    }

    # Optional: get total count
    count_payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "sale.order",
            "method": "web_search_read",
            "args": [],
            "kwargs": {
                "domain": domain,
                "specification": {"id": {}},
                "limit": 1,
                "context": context,
            },
        },
        "id": 99,
    }
    count_resp = session.post(
        f"{ODOO_URL}/web/dataset/call_kw/sale.order/web_search_read",
        data=json.dumps(count_payload),
    )
    count_resp.raise_for_status()
    total_count = count_resp.json()["result"]["length"]
    print(f"🔎 Total records to fetch for company {company_id}: {total_count}")

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
                    "order": "",
                    "context": context,
                    "count_limit": 100000,
                },
            },
            "id": 3,
        }
        resp = session.post(
            f"{ODOO_URL}/web/dataset/call_kw/sale.order/web_search_read",
            data=json.dumps(payload),
        )
        resp.raise_for_status()
        result = resp.json()["result"]
        records = result.get("records", [])
        all_records.extend(records)
        print(
            f"Fetched {len(records)} records for company {company_id}, total so far: {len(all_records)}/{total_count}"
        )
        if len(records) < batch_size:
            break
        offset += batch_size

    print(f"✅ Finished fetching for company {company_id}. Total fetched: {len(all_records)}")
    return all_records


# --------- Flatten Records into Rows ---------
def flatten_records(records):
    flat_rows = []
    for record in records:
        order_lines = record.get("order_line", [])
        for line in order_lines:
            order_id = line.get("order_id", {}) or {}
            flat_rows.append({
                "Order Reference": get_string_value(order_id.get("name")),
                "Sales Order Ref.": get_string_value(order_id.get("order_ref")),
                "Buyer": get_string_value(order_id.get("buyer_name")),
                "Brand Group": get_string_value(order_id.get("buyer_name"), "brand"),
                "Buying House": get_string_value(order_id.get("buying_house")),
                "Company": get_string_value(order_id.get("company_id")),
                "Customer": get_string_value(order_id.get("partner_id")),
                "Customer Group": get_string_value(order_id.get("partner_id"), "group"),
                "Order Date": get_string_value(order_id.get("date_order")),
                "Sales Team": get_string_value(order_id.get("team_id")),
                "Salesperson": get_string_value(order_id.get("user_id")),
                "FG Category": get_string_value(line.get("product_template_id"), "fg_categ_type"),
                "Quantity": line.get("product_uom_qty", 0),
                "Total": line.get("price_total", 0),
                "Slider Code (SFG)": get_string_value(line.get("slidercodesfg")),
                "LC Number": get_string_value(order_id.get("lc_number")),
                "Payment Terms": get_string_value(order_id.get("payment_term_id")),
                "Status": get_string_value(order_id.get("state")),
            })
    return flat_rows


def paste_to_gsheet(df, sheet_name):
    try:
        worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)
        
        if df.empty:
            print(f"⚠️ Skip: {sheet_name} is empty")
            return

        # Clear range A:R (assuming up to 18 columns)
        worksheet.batch_clear(["A:R"])

        # Paste the dataframe starting from A1
        set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

        # Add timestamp to S1
        local_time = datetime.now(pytz.timezone("Asia/Dhaka")).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update("S1", [[f"Last Updated: {local_time}"]])

        print(f"✅ Data pasted to {sheet_name} and timestamp updated in S1")
    except Exception as e:
        print(f"❌ Error pasting to {sheet_name}: {e}")
        raise


# --------- Main ---------
if __name__ == "__main__":
    uid = odoo_login()
    company_map = [(1, "OA_ITEM_DF_ZIP"), (3, "OA_ITEM_DF_MT")]

    MAX_RETRIES = 2  # Number of retries per company

    for company_id, sheet_tab in company_map:
        print(f"\n{'='*50}")
        print(f"Processing data for Company {company_id}...")

        retries = 0
        while retries < MAX_RETRIES:
            try:
                records = fetch_sale_orders_for_company(uid, company_id)
                flat_rows = flatten_records(records)
                df = pd.DataFrame(flat_rows)

                if df.empty:
                    print(f"⚠️ No data for Company {company_id}")
                    break

                # Automatically find numeric columns for aggregation
                numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

                # Automatically find non-numeric columns for group by
                group_cols = [col for col in df.columns if col not in numeric_cols]

                # Create aggregation dictionary dynamically (sum for numbers)
                agg_dict = {col: "sum" for col in numeric_cols}

                # Group by ALL non-numeric columns
                df_grouped = df.groupby(group_cols, as_index=False).agg(agg_dict).round(2)

                paste_to_gsheet(df_grouped, sheet_tab)
                print(f"✅ Finished processing Company {company_id}")
                break  # Success, exit retry loop

            except Exception as e:
                retries += 1
                print(f"❌ Error processing Company {company_id} (attempt {retries}/{MAX_RETRIES}): {e}")
                if retries == MAX_RETRIES:
                    print(f"⚠️ Failed to process Company {company_id} after {MAX_RETRIES} attempts.")
