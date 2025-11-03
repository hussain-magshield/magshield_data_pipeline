# ==============================
#  Organisations Extraction Script with Linked Contacts Count + Parallel Follow
# ==============================
import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
import os
import logging

# ==============================
#  Logging Configuration
# ==============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==============================
#  Load ENV
# ==============================
def load_env_config(file_path="env.yaml"):
    """
    Local development ke liye env.yaml read karega
    Production (Azure) mein env variable se read karega
    """
    config = {}

    # 1. Check if running locally and file exists
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            config = yaml.safe_load(f) or {}

    # 2. Environment variables (Azure)
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
#  Helper: Safe GET with exponential backoff
# ==============================
def safe_get(url, params=None, max_retries=5, timeout=60):
    backoff = 2
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, auth=auth, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (ChunkedEncodingError, ConnectionError, Timeout) as e:
            wait_time = backoff ** attempt
            logging.warning(f"Network error on attempt {attempt+1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Max retries reached. Skipping URL: {url}")
                return None
        except requests.HTTPError as e:
            logging.error(f"HTTP error: {e}")
            return None
    return None

# ==============================
#  Fetch all Organisations
# ==============================
def fetch_organisations():
    orgs = []
    skip = 0
    top = 500
    while True:
        params = {"skip": skip, "top": top}
        resp = safe_get(f"{BASE_URL}/Organisations", params=params)
        if not resp:
            break
        data = resp.json()
        if not data:
            break
        orgs.extend(data)
        skip += top
        logging.info(f"Organisations fetched so far: {len(orgs)}")
    return orgs

 

# ==============================
#  Utility: Clean text
# ==============================
def clean_text(value):
    if isinstance(value, str):
        return value.replace("\r", " ").replace("\n", " ").strip()
    return value

from datetime import datetime

def format_date_only(date_str):
    if not date_str:
        return ""
    try:
        # Input format: "2022-09-23 03:42:25"
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%m/%d/%Y")  # Output: 10/27/2025
    except ValueError:
        return date_str 
# ==============================
#  Transform API data to CSV rows
# ==============================
def transform_organisations(orgs):
    rows = []
    for org in orgs:
        cf = {c["FIELD_NAME"]: c.get("FIELD_VALUE") for c in org.get("CUSTOMFIELDS", [])}
        linked_contacts_count = sum(1 for l in org.get("LINKS", []) if l.get("LINK_OBJECT_NAME") == "Contact")
        org_id = org.get("ORGANISATION_ID")
        focus_org = bool(cf.get("Active__c", False))

        rows.append({
            "Organization ID": org_id,
            "Organization Name": clean_text(org.get("ORGANISATION_NAME", "")),
            "Date Created": format_date_only(org.get("DATE_CREATED_UTC")),
            "Linked Contacts Count": linked_contacts_count,
            "Focus Organization": focus_org,
            "Call Frequency": cf.get("Call_Frequency__c", ""),
            "Industry": cf.get("Industry__c", ""),
            "Region": cf.get("Region__c", ""),
            "Customer Type": cf.get("Sales_Methodology_Type__c", ""),
            "Organization Type": cf.get("Organization_Type__c", ""),
            "Billing Country": org.get("ADDRESS_BILLING_COUNTRY", ""),
            # "Organization Name Clean": clean_text(org.get("ORGANISATION_NAME", "")).lower(),
            # "DateOnlyCheck": org.get("DATE_CREATED_UTC", "").split(" ")[0] if org.get("DATE_CREATED_UTC") else "",
            # "Cumulative Active Focus Org": ""
        })
    return rows

# ==============================
#  Main Execution Function
# ==============================

    
def main_organisation():
    logging.info("Starting Organisation export process...")
    organisations = fetch_organisations()

    # Check if there are any organisations
    if not organisations:
        logging.warning("No organisations found. Skipping export.")
        return None

    logging.info("Fetching Focus Organization status in parallel...")
    

    rows = transform_organisations(organisations)

    # Double-check after transformation
    if not rows:
        logging.warning("No transformed data for organisations. Skipping export.")
        return None

     
    output_file = os.path.join("/tmp", "Organisations BRP.xlsx")

    df = pd.DataFrame(rows)
    df = df.drop_duplicates()

    df.to_excel(
        output_file,
        index=False,
        engine="openpyxl"
    )
    logging.info(f"Exported {len(rows)} organisations to {output_file}")
    return output_file
