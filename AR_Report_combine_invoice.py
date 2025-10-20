import os
import json
import re
import time
import base64
import requests
import pandas as pd
import gspread
import pytz
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# ================== CONFIG ==================
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

GOOGLE_CREDENTIALS_BASE64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1sPVTbTppdEn7_S2hFyYGTF2pUoyOx19NM4siqbCKFCw")
SHEET_TAB_NAME = os.getenv("SHEET_TAB_NAME", "Raw_Data")

MODEL = "ppc.report"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"
REPORT_TYPE = "report_all_invocie"
ALLOWED_COMPANY_IDS = [1, 3]

# ================== DYNAMIC DATES ==================
today = datetime.now().date()
DATE_FROM = today.replace(day=1).strftime("%Y-%m-%d")
DATE_TO = today.strftime("%Y-%m-%d")

# ================== GOOGLE SHEET SETUP ==================
creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_BASE64).decode("utf-8"))
creds = Credentials.from_service_account_info(creds_json, scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(creds)

def paste_to_gsheet(df):
    worksheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_TAB_NAME)
    if df.empty:
        print(f"⚠️ Skip: {SHEET_TAB_NAME} DataFrame is empty.")
        return
    worksheet.batch_clear(["A:BM"])
    set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)

    local_tz = pytz.timezone("Asia/Dhaka")
    local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    worksheet.update("BN1", [[f"Last Updated: {local_time}"]])
    print(f"✅ Data pasted to Google Sheet ({SHEET_TAB_NAME}), timestamp: {local_time}")

# ================== ODOO FETCH LOGIC ==================
def run_odoo_fetch():
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

    # Step 1: Login
    login_url = f"{ODOO_URL}/web/session/authenticate"
    login_payload = {"jsonrpc": "2.0","params":{"db": DB,"login": USERNAME,"password": PASSWORD}}
    resp = session.post(login_url, json=login_payload)
    resp.raise_for_status()
    login_result = resp.json()
    uid = login_result.get("result", {}).get("uid")
    if not uid:
        raise Exception(f"❌ Login failed: {resp.text}")
    print("✅ Logged in, UID =", uid)

    # Step 2: Get CSRF token
    resp = session.get(f"{ODOO_URL}/web")
    match = re.search(r'var odoo = {\s*csrf_token: "([A-Za-z0-9]+)"', resp.text)
    csrf_token = match.group(1) if match else None
    if not csrf_token:
        raise Exception("❌ Failed to extract CSRF token")
    print("✅ CSRF token =", csrf_token)

    # Step 3: Onchange (fetch defaults)
    onchange_url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/onchange"
    onchange_payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": "onchange",
            "args": [[], {}, [], {
                "report_type": {}, "date_from": {}, "date_to": {},
                "all_buyer_list": {"fields": {"display_name": {}}},
                "all_Customer": {"fields": {"display_name": {}}}
            }],
            "kwargs": {"context": {"lang": "en_US","tz": "Asia/Dhaka","uid": uid,"allowed_company_ids": ALLOWED_COMPANY_IDS}}
        }
    }
    resp = session.post(onchange_url, json=onchange_payload)
    resp.raise_for_status()
    print("✅ Onchange defaults received")

    # Step 4: Save wizard
    web_save_url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/web_save"
    web_save_payload = {
        "id": 2,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": "web_save",
            "args": [[], {"report_type": REPORT_TYPE, "date_from": DATE_FROM, "date_to": DATE_TO, "all_buyer_list": [], "all_Customer": []}],
            "kwargs": {
                "context": {"lang": "en_US","tz": "Asia/Dhaka","uid": uid,"allowed_company_ids": ALLOWED_COMPANY_IDS},
                "specification": {
                    "report_type": {}, "date_from": {}, "date_to": {},
                    "all_buyer_list": {"fields": {"display_name": {}}},
                    "all_Customer": {"fields": {"display_name": {}}}
                }
            }
        }
    }
    resp = session.post(web_save_url, json=web_save_payload)
    resp.raise_for_status()
    wizard_id = resp.json().get("result", [{}])[0].get("id")
    if not wizard_id:
        raise Exception(f"❌ Wizard creation failed: {resp.text}")
    print("✅ Wizard saved, ID =", wizard_id)

    # Step 5: Trigger report generation
    call_button_url = f"{ODOO_URL}/web/dataset/call_button"
    call_button_payload = {
        "id": 3,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": REPORT_BUTTON_METHOD,
            "args": [[wizard_id]],
            "kwargs": {"context": {"lang": "en_US","tz": "Asia/Dhaka","uid": uid,"allowed_company_ids": ALLOWED_COMPANY_IDS}}
        }
    }
    resp = session.post(call_button_url, json=call_button_payload)
    resp.raise_for_status()
    report_info = resp.json().get("result", {})
    report_name = report_info.get("report_name")
    if not report_name:
        raise Exception(f"❌ Failed to generate report: {resp.text}")
    print("✅ Report generated:", report_name)

    # Step 6: Download file with retries
    download_url = f"{ODOO_URL}/report/download"
    options = {"date_from": DATE_FROM, "date_to": DATE_TO}
    context = {"lang": "en_US","tz": "Asia/Dhaka","uid": uid,"allowed_company_ids": ALLOWED_COMPANY_IDS}
    report_path = f"/report/xlsx/{report_name}/{wizard_id}?options={json.dumps(options)}&context={json.dumps(context)}"
    download_payload = {"data": json.dumps([report_path, "xlsx"]),"context": json.dumps(context),"token": "dummy","csrf_token": csrf_token}
    headers = {"X-CSRF-Token": csrf_token, "Referer": f"{ODOO_URL}/web"}

    filename = f"All_Companies_{REPORT_TYPE}_{DATE_FROM}_to_{DATE_TO}.xlsx"
    success = False
    for attempt in range(4):
        try:
            resp = session.post(download_url, data=download_payload, headers=headers, timeout=60)
            if resp.status_code == 200 and "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in resp.headers.get("content-type", ""):
                with open(filename, "wb") as f:
                    f.write(resp.content)
                print(f"✅ Report downloaded: {filename}")
                success = True
                break
            else:
                print(f"❌ Download attempt {attempt+1} failed", resp.status_code, resp.text[:500])
        except Exception as e:
            print(f"❌ Download attempt {attempt+1} failed with exception: {e}")
        if attempt < 3:
            print("Retrying in 5 seconds...")
            time.sleep(5)

    if not success:
        raise Exception("❌ All download attempts failed")

    # Step 7: Paste to Google Sheet
    df = pd.read_excel(filename)
    paste_to_gsheet(df)

if __name__ == "__main__":
    MAX_RETRIES = 10  # Total retries for the whole fetch & paste
    retries = 0

    while retries < MAX_RETRIES:
        try:
            run_odoo_fetch()  # Attempt the whole fetch & paste
            print("✅ Odoo fetch and Google Sheet update completed successfully.")
            break  # Success, exit retry loop
        except Exception as e:
            retries += 1
            print(f"❌ Attempt {retries}/{MAX_RETRIES} failed: {e}")
            if retries < MAX_RETRIES:
                print("⏳ Retrying in 10 seconds...")
                time.sleep(10)
            else:
                print(f"⚠️ Failed after {MAX_RETRIES} attempts. Exiting.")
                raise