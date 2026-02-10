import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
import os
import logging
from datetime import datetime
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

    # 1. Local file
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            config = yaml.safe_load(f) or {}

    # 2. Environment variables (Azure)
    for key in ["INSIGHTLY_API_KEY", "CLIENT_ID", "TENANT_ID"]:
        if os.environ.get(key):
            config[key] = os.environ.get(key)

    return config

env = load_env_config()

API_KEY = env.get("INSIGHTLY_API_KEY")
CLIENT_ID = env.get("CLIENT_ID")
TENANT_ID = env.get("TENANT_ID")


BASE_URL = "https://api.na1.insightly.com/v3.1"
auth = HTTPBasicAuth(API_KEY, "")

# ==============================
#  Helper: Safe GET with retries
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
                time.sleep(wait_time)
            else:
                logging.error("Max retries reached. Skipping.")
                return None
        except requests.HTTPError as e:
            logging.error(f"HTTP error: {e}")
            return None
    return None

# ==============================
#  Fetch all Quotations
# ==============================
def fetch_all_quotations():
    quotations = []
    skip = 0
    top = 500
    while True:
        params = {
            "brief": "false",
            "skip": skip,
            "top": top,
            "count_total": "true"
        }
        resp = safe_get(f"{BASE_URL}/Quotation", params=params)
        if not resp:
            break
        data = resp.json()
        if not data:
            break
        quotations.extend(data)
        logging.info(f"Fetched {len(data)} records (total {len(quotations)})")

        total_count = int(resp.headers.get("X-Total-Count", len(quotations)))
        if len(quotations) >= total_count:
            break
        skip += top
    return quotations

# ==============================
#  Parallel Fetch Helpers
# ==============================
def fetch_opportunity(opportunity_id):
    if not opportunity_id:
        return opportunity_id, ""
    url = f"{BASE_URL}/Opportunities/{opportunity_id}"
    resp = safe_get(url)
    if resp and resp.status_code == 200:
        return opportunity_id, resp.json().get("OPPORTUNITY_NAME", "")
    return opportunity_id, ""

def fetch_organisation(organisation_id):
    if not organisation_id:
        return organisation_id, ""
    url = f"{BASE_URL}/Organisations/{organisation_id}"
    resp = safe_get(url)
    if resp and resp.status_code == 200:
        return organisation_id, resp.json().get("ORGANISATION_NAME", "")
    return organisation_id, ""

def fetch_contact(contact_id):
    if not contact_id:
        return contact_id, ""
    url = f"{BASE_URL}/Contacts/{contact_id}"
    resp = safe_get(url)
    if resp and resp.status_code == 200:
        data = resp.json()
        full_name = f'{data.get("FIRST_NAME", "")} {data.get("LAST_NAME", "")}'.strip()
        return contact_id, full_name
    return contact_id, ""

# ==============================
#  Bulk Prefetch (Parallel)
# ==============================
def prefetch_related_data(quotations):
    opp_ids = set()
    org_ids = set()
    contact_ids = set()

    for q in quotations:
        if q.get("OPPORTUNITY_ID"):
            opp_ids.add(q["OPPORTUNITY_ID"])
        if q.get("ORGANISATION_ID"):
            org_ids.add(q["ORGANISATION_ID"])
        for c in q.get("CUSTOMFIELDS", []):
            if c["FIELD_NAME"] == "Sales_Person__c" and c.get("FIELD_VALUE"):
                contact_ids.add(c["FIELD_VALUE"])

    opportunity_cache = {}
    organisation_cache = {}
    contact_cache = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_opportunity, oid): ("opp", oid) for oid in opp_ids}
        futures.update({executor.submit(fetch_organisation, oid): ("org", oid) for oid in org_ids})
        futures.update({executor.submit(fetch_contact, cid): ("contact", cid) for cid in contact_ids})

        for future in as_completed(futures):
            typ, id_ = futures[future]
            try:
                id_val, name_val = future.result()
                if typ == "opp":
                    opportunity_cache[id_val] = name_val
                elif typ == "org":
                    organisation_cache[id_val] = name_val
                elif typ == "contact":
                    contact_cache[id_val] = name_val
            except Exception as e:
                logging.error(f"Failed fetching {typ} {id_}: {e}")

    return opportunity_cache, organisation_cache, contact_cache


def format_date(date_str):
    if not date_str:
        return "" 
    try: 
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d-%b-%y %-I:%M %p") # e.g. 25-Aug-25 8:41 PM 
    except ValueError: 
        return date_str
# ==============================
#  Main Execution
# ==============================


 
def main_quote():
    quotations = fetch_all_quotations()

    if not quotations:
        logging.warning("No quotations found. Skipping file generation.")
        return None

    logging.info("Prefetching related Opportunity, Organisation, and Contact data...")
    opportunity_cache, organisation_cache, contact_cache = prefetch_related_data(quotations)
    logging.info(f"Prefetched {len(opportunity_cache)} opportunities, {len(organisation_cache)} orgs, {len(contact_cache)} contacts")

    rows = []
    for q in quotations:
        cf = {c["FIELD_NAME"]: c.get("FIELD_VALUE") for c in q.get("CUSTOMFIELDS", [])}

        row = {
           
            "Record ID": q.get("QUOTE_ID"),
            "Quote Number": q.get("QUOTATION_NUMBER"), 
            "Status": q.get("QUOTE_STATUS"), "Quote Name": q.get("QUOTATION_NAME"),
            "Subtotal": q.get("SUBTOTAL"), "Total Price": q.get("TOTAL_PRICE"), 
            "Expiration Date": q.get("QUOTATION_EXPIRATION_DATE"),
            "GST %": cf.get("GST_Percentage__c", ""), "Tax": cf.get("Tax__c", ""), 
            "Grand Total": cf.get("Grand_Total__c", q.get("GRAND_TOTAL", "")),
            "Trade Tariff": cf.get("Trade_Tariff__c", ""), "Grand Total w/ Tariff": cf.get("Grand_Total_Tariff__c", ""),
            "MagShield Selling Entity": cf.get("MagShield_Selling_Entity__c", ""), 
            "Sales Person Id": str(cf.get("Sales_Person__c", "")),
            "Sales Person": contact_cache.get(cf.get("Sales_Person__c"), ""), 
            "Billing Country": q.get("ADDRESS_BILLING_COUNTRY"),
            "Currency": q.get("QUOTATION_CURRENCY_CODE"), 
            "Discount": q.get("DISCOUNT"), 
            "Organization Name": q.get("ORGANISATION_NAME") or organisation_cache.get(q.get("ORGANISATION_ID"), ""),
            "Record ID_1": q.get("ORGANISATION_ID"),
            # "Date Created": q.get("DATE_CREATED_UTC"),
            # # "Date Updated": q.get("DATE_UPDATED_UTC"),
            "Date Created": format_date(q.get("DATE_CREATED_UTC")), 
            "Date Updated": format_date(q.get("DATE_UPDATED_UTC")),
            "Opportunity Name": q.get("OPPORTUNITY_NAME") or opportunity_cache.get(q.get("OPPORTUNITY_ID"), ""),
            "Shipping_Terms__c": cf.get("Shipping_Terms__c", ""),
            "ADDRESS_SHIPPING_COUNTRY": q.get("ADDRESS_SHIPPING_COUNTRY", "")

            
            # "Opportunity ID": q.get("OPPORTUNITY_ID"),
        }
        rows.append(row)

     
    output_file = os.path.join("/tmp", "Quotes.xlsx")
    if rows:
        df = pd.DataFrame(rows)
        df = df.drop_duplicates()

        df.to_excel(output_file, index=False, engine="openpyxl")
        logging.info(f"Exported {len(rows)} quotations to {output_file}")
        return output_file
    else:
        logging.warning("No rows to export. File will not be created.")
        return None
