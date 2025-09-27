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
from typing import Dict, Any, Optional, List

# --------- Environment Variables ---------
ODOO_URL = os.getenv("ODOO_URL")  # e.g. https://taps.odoo.com
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USERNAME = os.getenv("ODOO_USERNAME")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD")
GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1sPVTbTppdEn7_S2hFyYGTF2pUoyOx19NM4siqbCKFCw")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Raw_Data")

# --------- Session ---------
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

# --------- Google creds (fixed decode) ---------
if not GOOGLE_CREDENTIALS_BASE64:
    raise EnvironmentError("Missing GOOGLE_CREDENTIALS_BASE64 environment variable")

try:
    creds_text = base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode("utf-8")
    creds_json = json.loads(creds_text)
except Exception as e:
    raise RuntimeError(f"Failed to decode GOOGLE_CREDENTIALS_BASE64: {e}")

creds = Credentials.from_service_account_info(
    creds_json,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
gc = gspread.authorize(creds)

# --------- Odoo login ---------
def odoo_login() -> int:
    if not all([ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD]):
        raise EnvironmentError("One or more ODOO_* env vars missing")
    url = f"{ODOO_URL}/web/session/authenticate"
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD},
        "id": 1,
    }
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    result = resp.json().get("result")
    if not result or "uid" not in result:
        raise RuntimeError(f"Odoo login failed: {resp.text}")
    uid = result["uid"]
    print(f"✅ Logged in to Odoo, uid: {uid}")
    return uid

# --------- Generic Odoo RPC caller for web_search_read-like ops ---------
def call_odoo_rpc(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{ODOO_URL}{path}"
    resp = session.post(url, data=json.dumps(payload))
    resp.raise_for_status()
    j = resp.json()
    if "error" in j:
        raise RuntimeError(f"Odoo RPC error: {j['error']}")
    return j

def odoo_web_search_read(model: str,
                         specification: Dict[str, Any],
                         domain: Optional[List[Any]] = None,
                         uid: Optional[int] = None,
                         offset: int = 0,
                         limit: int = 2000,
                         order: str = "") -> List[Dict[str, Any]]:
    """
    Generic paginated web_search_read (works like the one you observed).
    Returns list of records.
    """
    all_records = []
    # Ensure domain and context default
    if domain is None:
        domain = [["state", "=", "posted"]]

    while True:
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": model,
                "method": "web_search_read",
                "args": [],
                "kwargs": {
                    "specification": specification,
                    "offset": offset,
                    "limit": limit,
                    "order": order,
                    "context": {
                        "lang": "en_US",
                        "tz": "Asia/Dhaka",
                        "uid": uid,
                        "allowed_company_ids": [1, 3],
                        "bin_size": True,
                        "current_company_id": 1,
                    },
                    "count_limit": 100000,
                    "domain": domain,
                },
            },
            "id": 3,
        }
        j = call_odoo_rpc("/web/dataset/call_kw/{}/web_search_read".format(model.replace(".", "/")), payload)
        result = j.get("result", {})
        records = result.get("records", [])
        all_records.extend(records)
        print(f"Fetched {len(records)} records (offset {offset}); total so far: {len(all_records)}")
        if len(records) < limit:
            break
        offset += limit
    print(f"✅ Finished fetching model {model}. Total: {len(all_records)}")
    return all_records

# --------- Other Odoo endpoints you listed (search_panel_select_range, get_fields, namelist, formats, search_read) ---------
def odoo_search_panel_select_range(field_name: str, uid: int, search_domain=None, limit: int = 200):
    payload = [{
        "id": 13,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "combine.invoice",
            "method": "search_panel_select_range",
            "args": [field_name],
            "kwargs": {
                "category_domain": [],
                "context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid, "allowed_company_ids": [1, 3]},
                "enable_counters": True,
                "expand": False,
                "filter_domain": [],
                "hierarchize": True,
                "limit": limit,
                "search_domain": search_domain or [["state", "=", "posted"]],
            },
        },
    }]
    return call_odoo_rpc("/web/dataset/call_kw/combine.invoice/search_panel_select_range", payload)

def odoo_get_fields(uid: int):
    payload = {"jsonrpc": "2.0", "id": 17, "params": {"model": "combine.invoice", "import_compat": False}}
    return call_odoo_rpc("/web/export/get_fields", payload)

def odoo_namelist(uid: int):
    payload = {"jsonrpc": "2.0", "id": 18, "params": {}}
    # In browser flow it was likely /web/export/namelist; match that route:
    return call_odoo_rpc("/web/export/namelist", payload)

def odoo_formats():
    payload = {"id": 15, "jsonrpc": "2.0", "method": "call", "params": {}}
    return call_odoo_rpc("/web/export/formats", payload)

def odoo_search_read_ir_exports(uid: int):
    payload = {
        "id": 16,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "ir.exports",
            "method": "search_read",
            "args": [],
            "kwargs": {
                "context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid, "allowed_company_ids": [1, 3]},
                "domain": [["resource", "=", "combine.invoice"]],
                "fields": []
            }
        }
    }
    return call_odoo_rpc("/web/dataset/call_kw/ir.exports/search_read", payload)

# --------- Helper to safely extract display strings (kept from your original) ---------
def get_string_value(field, subfield=None):
    if isinstance(field, dict):
        if subfield:
            value = field.get(subfield)
            return get_string_value(value)
        if "display_name" in field:
            return str(field["display_name"] or "")
        # If it's a mapping, join values (fallback)
        return " ".join([str(v) for v in field.values()])
    elif isinstance(field, int):
        return str(field)
    elif field in (False, None):
        return ""
    return str(field)

# --------- Flatten arbitrary records using a field-spec extractor mapping ---------
def flatten_records(records: List[Dict[str, Any]], field_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    field_map: mapping of output_column -> either
       - string: field name in record (shallow)
       - tuple/list: (record_key, subfield) for nested object
       - callable: function(record) -> value
    """
    out = []
    for r in records:
        row = {}
        for out_col, spec in field_map.items():
            try:
                if callable(spec):
                    val = spec(r)
                elif isinstance(spec, (list, tuple)) and len(spec) == 2:
                    val = get_string_value(r.get(spec[0]), spec[1])
                else:
                    val = get_string_value(r.get(spec))
            except Exception:
                val = ""
            row[out_col] = val
        out.append(row)
    return out

# --------- Normalize and Group (keeps original behavior) ---------
def normalize_dates_and_group(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == "object":
            # try to coerce date-like strings
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.date

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    group_cols = [c for c in df.columns if c not in numeric_cols]
    if numeric_cols:
        return df.groupby(group_cols, dropna=False)[numeric_cols].sum().reset_index()
    else:
        # nothing numeric to sum: drop duplicates so each record appears once
        return df.drop_duplicates().reset_index(drop=True)

# --------- Rename columns using namelist mapping (label friendly) ---------
def apply_label_map(df: pd.DataFrame, namelist: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    namelist: list of {"name": "<field>", "label": "<Label>"}
    """
    name_to_label = {entry["name"]: entry["label"] for entry in namelist}
    # rename only those columns present
    new_cols = {col: name_to_label.get(col, col) for col in df.columns}
    return df.rename(columns=new_cols)

# --------- Paste to Google Sheet (keeps original behavior) ---------
def paste_to_gsheet(df: pd.DataFrame, sheet_key: str = GOOGLE_SHEET_ID, tab_name: str = SHEET_TAB_NAME):
    worksheet = gc.open_by_key(sheet_key).worksheet(tab_name)
    if df.empty:
        print(f"⚠️ Skip: {tab_name} DataFrame is empty.")
        return
    worksheet.batch_clear(["A:AG"])
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("AI1", [[f"Last Updated: {local_time}"]])
    print(f"✅ Data pasted to Google Sheet ({tab_name}), timestamp: {local_time}")

# --------- Example usage helpers (adapt these to the payloads you showed) ---------
def fetch_combine_invoice_example(uid: int):
    """
    Recreates the web_search_read you pasted for combine.invoice
    """
    specification = {
        "name": {},
        "create_date": {},
        "report_date": {},
        "delivery_date": {},
        "create_uid": {"fields": {"display_name": {}}},
        "partner_id": {"fields": {"display_name": {}}},
        "invoice_date": {},
        "invoice_payment_term_id": {"fields": {"display_name": {}}},
        "invoice_incoterm_id": {"fields": {"display_name": {}}},
        "sales_person": {"fields": {"display_name": {}}},
        "team_id": {"fields": {"display_name": {}}},
        "buyer_name": {"fields": {"display_name": {}}},
        "z_total_q": {},
        "m_total_q": {},
        "z_total": {},
        "m_total": {},
        "qty_total": {},
        "amount_total": {},
        "state": {},
    }
    domain = [["state", "=", "posted"]]
    records = odoo_web_search_read("combine.invoice", specification, domain=domain, uid=uid, limit=80)
    return records

def fetch_combine_invoice_line_example(uid: int, start_date="2025-04-01", end_date="2025-04-30"):
    """
    Recreate your combine.invoice.line fetch with the more generic caller:
    """
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
    domain = ["&", ["parent_state", "=", "posted"], "&", ["invoice_date", ">=", start_date], ["invoice_date", "<=", end_date]]
    records = odoo_web_search_read("combine.invoice.line", specification, domain=domain, uid=uid, limit=2000)
    return records

# --------- Quick mapping for flattening the combine.invoice.line into columns you used earlier ---------
COMBINE_INV_LINE_FIELD_MAP = {
    "Sale Order Ref": ("sale_order_line", "order_id"),
    "Customer Invoice Items": "invoice_id",
    "Buying House": ("buying_house", None),
    "Category": ("product_uom_category_id", None),
    "Company": ("company_id", None),
    "Invoice Date": "invoice_date",
    "Status": "parent_state",
    "Quantity": lambda r: r.get("quantity", 0),
    "Total": lambda r: r.get("price_total", 0),
    "Item": "fg_categ_type",
    "Sales Ots Line ID": ("sales_ots_line", "id"),
    "Marketing Ots Line ID": ("marketing_ots_line", "id"),
    "LC No": ("invoice_id", "lc_no"),
    "LC Date": ("invoice_id", "lc_date"),
    "Payment Terms": ("invoice_id", "invoice_payment_term_id"),
    "Buyer": ("buyer_id", None),
    "Buyer Group": ("buyer_group", None),
    "Customer": ("customer_id", None),
    "Customer Group": ("customer_group", None),
    "Sales Person": ("sales_person", None),
    "Team": ("team_id", None),
    "Country": ("country_id", None),
}

# --------- The main runnable example (adjust as you like) ---------
if __name__ == "__main__":
    uid = odoo_login()

    # Example A: fetch combine.invoice (like your browser web_search_read)
    inv_records = fetch_combine_invoice_example(uid)
    print(f"combine.invoice records fetched: {len(inv_records)}")

    # Example B: fetch combine.invoice.line (your original target)
    recs = fetch_combine_invoice_line_example(uid, start_date="2025-04-01", end_date="2025-04-30")
    flat_rows = flatten_records(recs, COMBINE_INV_LINE_FIELD_MAP)
    df = pd.DataFrame(flat_rows)

    # If the Odoo returned nested date strings, try converting them; keep consistent with your normalization
    grouped_df = normalize_dates_and_group(df)

    # Optional: fetch namelist mapping from Odoo and apply label names
    try:
        namelist_response = odoo_namelist(uid)  # This returns the same shape you included earlier
        # The actual path you used in browser may return the list under result; handle both:
        namelist_list = namelist_response.get("result") if isinstance(namelist_response, dict) and namelist_response.get("result") else namelist_response
        if isinstance(namelist_list, dict) and "result" in namelist_list:
            namelist_list = namelist_list["result"]
        if isinstance(namelist_list, list) and len(namelist_list) > 0 and isinstance(namelist_list[0], dict) and "label" in namelist_list[0]:
            grouped_df = apply_label_map(grouped_df, namelist_list)
            print("Applied label map from namelist.")
    except Exception as e:
        print(f"Could not fetch/parse namelist: {e}")

    # paste to sheet
    paste_to_gsheet(grouped_df)
