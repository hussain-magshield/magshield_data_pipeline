import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import yaml
import logging
from datetime import datetime
# ==============================
#  Logging Config
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
def load_env_config(file_path="env.yaml"):
    """
    Local development ke liye env.yaml read karega
    Production (Azure) mein env variable se read karega
    """
    config = {}
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            config = yaml.safe_load(f) or {}

    for key in ["INSIGHTLY_API_KEY", "CLIENT_ID", "TENANT_ID", "REFRESH_TOKEN"]:
        if os.environ.get(key):
            config[key] = os.environ.get(key)
    return config

env = load_env_config()

API_KEY = env.get("INSIGHTLY_API_KEY")
CLIENT_ID = env.get("CLIENT_ID")
TENANT_ID = env.get("TENANT_ID")
REFRESH_TOKEN = env.get("REFRESH_TOKEN")

BASE_URL = "https://api.na1.insightly.com/v3.1"
auth = HTTPBasicAuth(API_KEY, "")

# ==============================
#  Safe GET with retry
# ==============================
def safe_get(url, params=None, max_retries=5, timeout=60):
    backoff = 2
    for attempt in range(max_retries):
        try:
            r = requests.get(url, auth=auth, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except (ChunkedEncodingError, ConnectionError, Timeout) as e:
            wait = backoff ** attempt
            logging.warning(f"Network error on attempt {attempt+1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(wait)
            else:
                logging.error(f"Skipping {url} after {max_retries} failed attempts")
                return None
        except requests.HTTPError as e:
            logging.error(f"HTTP error: {e}")
            return None
    return None

# ==============================
#  Fetch all paged records
# ==============================
def fetch_all_paged(endpoint, top=500):
    records = []
    first_resp = safe_get(
        f"{BASE_URL}/{endpoint}",
        params={"top": 1, "count_total": "true", "brief": "false"}
    )
    if not first_resp:
        return records

    total_count = int(first_resp.headers.get("X-Total-Count", 0))
    total_pages = (total_count // top) + (1 if total_count % top != 0 else 0)
    logging.info(f"{endpoint}: {total_count} records, {total_pages} pages")

    def fetch_page(page_idx):
        skip = page_idx * top
        r = safe_get(f"{BASE_URL}/{endpoint}", params={"skip": skip, "top": top, "brief": "false"})
        return r.json() if r and r.status_code == 200 else []

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_page, i): i for i in range(total_pages)}
        for f in as_completed(futures):
            data = f.result()
            if data:
                records.extend(data)

    logging.info(f"{endpoint}: fetched {len(records)} records")
    return records

# ==============================
#  Lookup Builders
# ==============================
def build_users_lookup():
    users = fetch_all_paged("Users")
    return {
        str(u["USER_ID"]): f'{u.get("USER_ID")};{u.get("FIRST_NAME","")} {u.get("LAST_NAME","")}'
        for u in users
    }

def build_org_lookup():
    orgs = fetch_all_paged("Organisations")
    org_map = {}
    for o in orgs:
        org_id = str(o["ORGANISATION_ID"])
        org_name = o.get("ORGANISATION_NAME", "")
        cf = {c["FIELD_NAME"]: c.get("FIELD_VALUE") for c in o.get("CUSTOMFIELDS", [])}
        org_map[org_id] = {
            "name": org_name,
            "organization_type": cf.get("Organization_Type__c", ""),
            "region": cf.get("Region__c", "")
        }
    return org_map

# ==============================
#  Helpers
# ==============================
def clean_text(v):
    return v.replace("\r", " ").replace("\n", " ").strip() if isinstance(v, str) else v

def format_owner_for_invoice(owner_str):
    """Convert 'USER_ID;First Last' to 'First Last||USER_ID||User'."""
    if not owner_str:
        return ""
    parts = owner_str.split(";")
    if len(parts) == 2:
        return f"{parts[0]};{parts[1]}"
    return owner_str

def format_date_ui(date_str):
    """Convert 'YYYY-MM-DD...' to 'DD/MM/YYYY' like UI."""
    if not date_str:
        return ""
    try:
        d = datetime.strptime(date_str.split(" ")[0], "%Y-%m-%d")
        return d.strftime("%d/%m/%Y")
    except Exception:
        return date_str

# ==============================
#  MAIN: Fetch Invoices
# ==============================
def main_invoice_export():
    logging.info("Building lookup tables...")
    user_lookup = build_users_lookup()
    org_lookup = build_org_lookup()
    logging.info("Fetching invoice data...")
    invoices = fetch_all_paged("Invoice_History__c")

    if not invoices:
        logging.warning("No Invoice records found.")
        return None

    rows = []
    for inv in invoices:
        record_id = inv.get("RECORD_ID")
        record_name = inv.get("RECORD_NAME")
        owner_id = str(inv.get("OWNER_USER_ID") or "")
        owner_name = user_lookup.get(owner_id, "")
        owner_formatted = format_owner_for_invoice(owner_name)

        cf = {c["FIELD_NAME"]: c.get("FIELD_VALUE") for c in inv.get("CUSTOMFIELDS", [])}
 
        invoiced_org_id = str(cf.get("Invoiced_Organization__c") or "")
        site_org_id = str(cf.get("Site_Organization__c") or "")   
        channel_partner_id = str(cf.get("Channel_Partner_Invoiced__c") or "")

        invoiced_org = org_lookup.get(invoiced_org_id, {})
       
        channel_partner_org = org_lookup.get(channel_partner_id, {})
        site_org_id = str(cf.get("Site_Name_Invoice__c") or "")
        site_org = org_lookup.get(site_org_id, {})
        rows.append({
            "Invoice Number": clean_text(record_name),
            "Record ID": record_id,
            "Owner": clean_text(owner_formatted),
            "Invoice Date": format_date_ui(cf.get("Invoice_Date__c", "")),
            "Item ID": clean_text(cf.get("Invoiced_Item__c", "")),
            "Invoiced Amount": clean_text(cf.get("Invoiced_Amount__c", "")),
            "Invoice Currency": clean_text(cf.get("Invoice_Currency__c", "")),
            "PO Number": clean_text(cf.get("PO_Number__c", "")),
            "Item Quantity": clean_text(cf.get("Item_Quantity__c", "")),
            "Product Type": clean_text(cf.get("Invoiced_Product_Type__c", "")),
            "Equipment Type": clean_text(cf.get("Invoiced_Product_for_Equipment_Type__c", "")),

            "Entity Owning Equipment": clean_text(invoiced_org.get("name", "")),
            "Organization Type (Entity)": clean_text(invoiced_org.get("organization_type", "")),
            "Region (Entity)": clean_text(invoiced_org.get("region", "")),
            "Site Name": clean_text(site_org.get("name", "")),
            "Organization Type (Site)": clean_text(site_org.get("organization_type", "")),
            "Region (Site)": clean_text(site_org.get("region", "")),

            "Channel Partner": clean_text(channel_partner_org.get("name", "")),
            "Organization Type (Channel Partner)": clean_text(channel_partner_org.get("organization_type", "")),
            "Region (Channel Partner)": clean_text(channel_partner_org.get("region", "")),

            "Invoice #": clean_text(cf.get("Invoice_Num__c", "")),
            "Invoiced Amount in CAD": clean_text(cf.get("Invoiced_Amount_in_CAD__c", "")),
        })
    
    output_file = os.path.join("/tmp", "invoice_records.xlsx") 
    
    df = pd.DataFrame(rows)
    df = df.drop_duplicates()

    df.to_excel(output_file, index=False, engine="openpyxl")
    logging.info(f" Exported {len(rows)} invoice records to {output_file}")
    return output_file

 
 