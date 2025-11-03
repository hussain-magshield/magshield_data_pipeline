import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import yaml
import logging

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
    first_resp = safe_get(f"{BASE_URL}/{endpoint}", params={"top": 1,  "count_total": "true", "brief": "false"})
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
#  Build user & organisation lookup
# ==============================
def build_users_lookup():
    users = fetch_all_paged("Users")
    return {
        str(u["USER_ID"]): f'{u.get("USER_ID")};{u.get("FIRST_NAME","")} {u.get("LAST_NAME","")}'
        for u in users
    }
def format_org_owner_site(owner_str):
    """Convert 'USER_ID;First Last' to 'First Last||USER_ID||User'."""
    if not owner_str:
        return ""
    parts = owner_str.split(";")
    if len(parts) == 2:
        return f"{parts[1]}||{parts[0]}||User"
    return owner_str

def build_org_lookup():
    orgs = fetch_all_paged("Organisations")
    return {str(o["ORGANISATION_ID"]): o.get("ORGANISATION_NAME", "") for o in orgs}

# ==============================
#  Clean text helper
# ==============================
def clean_text(v):
    return v.replace("\r", " ").replace("\n", " ").strip() if isinstance(v, str) else v

# ==============================
#  MAIN: Fetch Equipment
# ==============================
def main_equipment_export():
    user_lookup = build_users_lookup()
    org_lookup = build_org_lookup()
    equipments = fetch_all_paged("Equipment__c")

    if not equipments:
        logging.warning("No Equipment records found.")
        return None

    rows = []
    for e in equipments:
        record_id = e.get("RECORD_ID")
        record_name = e.get("RECORD_NAME")
        owner_id = str(e.get("OWNER_USER_ID") or "")
        date_created = e.get("DATE_CREATED_UTC")
        date_updated = e.get("DATE_UPDATED_UTC")
        owner_name = user_lookup.get(owner_id, "")

        # Convert custom fields list to dict for easy access
        cf = {c["FIELD_NAME"]: c.get("FIELD_VALUE") for c in e.get("CUSTOMFIELDS", [])}

        entity_org_id = str(cf.get("Entity_Owning_Equipment_Equipment__c") or "")
        site_org_id = str(cf.get("Site_Name_Equipment__c") or "")

        rows.append({
            "Record ID": record_id,
            "Equipment Mine - Make - Model": clean_text(record_name),
            "Owner": clean_text(owner_name),
            "Date Created": date_created,
            "Date Updated": date_updated,
            "Record ID_1": entity_org_id,
            "Entity Owning Equipment": clean_text(org_lookup.get(entity_org_id, "")),
            "Organization": format_org_owner_site(owner_name),
            "Record ID_2": site_org_id,
            "Site Name": clean_text(org_lookup.get(site_org_id, "")),
            "Organization Owner_3": format_org_owner_site(owner_name),
            "Equipment Type": clean_text(cf.get("Equipment_Type_Equipment__c", "")),
            "Equipment Make": clean_text(cf.get("Equipment_Make_Equipment__c", "")),
            "Equipment Model": clean_text(cf.get("Equipment_Model_Equipment__c", "")),
            "Equipment Quantity": cf.get("Equipment_Quantity_Equipment__c", ""),
            "Serial Number Notes": clean_text(cf.get("Serial_Number_Notes__c", "")),
            "Last_Date_of_Equipment_Details_Confirmed__c": cf.get("Last_Date_of_Equipment_Details_Confirmed__c", ""),
        })

    output_file = os.path.join("/tmp", "Equipment.xlsx")
   
    df = pd.DataFrame(rows)
    df = df.drop_duplicates()

    
    df.to_excel(output_file, index=False, engine="openpyxl")
    logging.info(f" Exported {len(rows)} equipment records to {output_file}")
    return output_file

 
