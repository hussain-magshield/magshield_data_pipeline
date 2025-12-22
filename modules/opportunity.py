import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import yaml
import logging

# ===========================
#   ENABLE LOGGING
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

def log_time(label, start):
    logging.info(f"{label}: {round(time.time() - start, 2)}s")


# ===========================
#  LOAD ENV
# ===========================
def load_env_config(file_path="env.yaml"):
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
BASE_URL = "https://api.na1.insightly.com/v3.1"
auth = HTTPBasicAuth(API_KEY, "")


# ===========================
#  SAFE GET
# ===========================
def safe_get(url, params=None, max_retries=5, timeout=60):
    backoff = 2
    for attempt in range(max_retries):
        try:
            r = requests.get(url, auth=auth, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(backoff ** attempt)
            else:
                return None
    return None


# ===========================
#  PAGINATED FETCH
# ===========================
def fetch_all_paged(endpoint, top=500):
    t0 = time.time()
    logging.info(f"Fetching {endpoint} ...")

    records = []
    resp = safe_get(f"{BASE_URL}/{endpoint}", params={"top": 1, "count_total": "true"})
    if not resp:
        logging.warning(f"Failed to fetch {endpoint}")
        return records

    total_count = int(resp.headers.get("X-Total-Count", 0))
    total_pages = (total_count // top) + (1 if total_count % top != 0 else 0)

    def fetch_page(page_idx):
        skip = page_idx * top
        r = safe_get(f"{BASE_URL}/{endpoint}", params={"skip": skip, "top": top})
        return r.json() if r and r.status_code == 200 else []

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(fetch_page, i) for i in range(total_pages)]
        for f in as_completed(futures):
            records.extend(f.result())

    logging.info(f"Fetched {len(records)} {endpoint} in {round(time.time() - t0, 2)}s")
    return records


# ===========================
#   BULK MAP BUILDERS
# ===========================
def build_pricebook_entry_map():
    entries = fetch_all_paged("PricebookEntry")
    return {str(e["PRICEBOOK_ENTRY_ID"]): str(e.get("PRODUCT_ID")) for e in entries}


def build_stage_map():
    stages = fetch_all_paged("PipelineStages")
    return {str(s["STAGE_ID"]): s.get("STAGE_NAME", "") for s in stages}


def build_opp_link_map():
    links = fetch_all_paged("OpportunityLinks")
    opp_links = {}
    for l in links:
        oid = str(l.get("OBJECT_ID"))
        if l.get("OBJECT_NAME") == "Opportunity":
            opp_links.setdefault(oid, []).append(l)
    return opp_links


def clean_text(v):
    return v.replace("\r", " ").replace("\n", " ").strip() if isinstance(v, str) else v


# ===========================
#   MAIN EXECUTION
# ===========================
def main_opportunity():
    
    total_start = time.time()
    logging.info(" Starting Opportunity Export...")

    t0 = time.time()
    orgs = {str(o["ORGANISATION_ID"]): o.get("ORGANISATION_NAME", "") for o in fetch_all_paged("Organisations")}
    log_time("Loaded Organisations", t0)

    t0 = time.time()
    users = {str(u["USER_ID"]): f'{u.get("USER_ID")};{u.get("FIRST_NAME","")} {u.get("LAST_NAME","")}'
             for u in fetch_all_paged("Users")}
    log_time("Loaded Users", t0)

    t0 = time.time()
    pricebooks = {str(p["PRICEBOOK_ID"]): p.get("NAME", "") for p in fetch_all_paged("Pricebook")}
    log_time("Loaded Pricebooks", t0)

    t0 = time.time()
    products = {str(p["PRODUCT_ID"]): p.get("PRODUCT_FAMILY", "") for p in fetch_all_paged("Product")}
    log_time("Loaded Products", t0)

    t0 = time.time()
    state_reason_map = {str(r["STATE_REASON_ID"]): r.get("STATE_REASON", "")
                        for r in fetch_all_paged("OpportunityStateReasons")}
    log_time("Loaded State Reasons", t0)

    # NEW MAPS
    t0 = time.time()
    stage_map = build_stage_map()
    log_time("Loaded Stage Map", t0)

    t0 = time.time()
    pricebook_entry_map = build_pricebook_entry_map()
    log_time("Loaded Pricebook Entry Map", t0)

    t0 = time.time()
    opp_link_map = build_opp_link_map()
    log_time("Loaded Opportunity Link Map", t0)

    t0 = time.time()
    line_items = fetch_all_paged("OpportunityLineItem")
    log_time("Loaded Opportunity Line Items", t0)

    # Build product map
    t0 = time.time()
    opp_product_map = {}
    for li in line_items:
        pid = pricebook_entry_map.get(str(li.get("PRICEBOOK_ENTRY_ID")))
        if pid:
            opp_product_map.setdefault(str(li["OPPORTUNITY_ID"]), []).append(pid)
    log_time("Mapped Products to Opportunities", t0)

    t0 = time.time()
    opportunities = fetch_all_paged("Opportunities")
    log_time("Loaded Opportunities", t0)

     
    t0 = time.time()
    rows = []

    for opp in opportunities:
        cf = {c["FIELD_NAME"]: c.get("FIELD_VALUE") for c in opp.get("CUSTOMFIELDS", [])}

        opp_id = str(opp["OPPORTUNITY_ID"])
        stage_name = stage_map.get(str(opp.get("STAGE_ID") or ""), "")

        # Site Name
        main_org = str(opp.get("ORGANISATION_ID") or "")
        site_links = opp_link_map.get(opp_id, [])
        site_names = [
            orgs.get(str(l["LINK_OBJECT_ID"])) for l in site_links
            if l.get("LINK_OBJECT_NAME") == "Organisation" and str(l["LINK_OBJECT_ID"]) != main_org
        ]
        site_name = " and ".join([s for s in site_names if s])

        product_ids = opp_product_map.get(opp_id, [])
        invoice_num = cf.get("Invoice_Number__c", "")
        po_number = cf.get("Purchase_Order__c", "")

        def base_row(pid=""):
            return {
                "Opportunity ID": opp_id,
                "Opportunity Name": clean_text(opp.get("OPPORTUNITY_NAME", "")),
                "Entity Owning Equipment": clean_text(orgs.get(str(cf.get("Entity_Owning_Equipment__c")), "")),
                "Site Name": site_name,
                "Channel Partner": clean_text(orgs.get(str(cf.get("Channel_Owner__c")), "")),
                "Date Created": opp.get("DATE_CREATED_UTC"),
                "Date Closed (Forecast)": opp.get("FORECAST_CLOSE_DATE"),
                "Date Closed (Actual)": opp.get("ACTUAL_CLOSE_DATE"),
                "Opportunity Value": opp.get("OPPORTUNITY_VALUE"),
                "Bid Currency": opp.get("BID_CURRENCY"),
                "Opportunity State": opp.get("OPPORTUNITY_STATE"),
                "Current Pipeline Stage": stage_name,
                "Expected Revenue": opp.get("OPPORTUNITY_VALUE"),
                "Date of Last Activity": opp.get("LAST_ACTIVITY_DATE_UTC"),
                "Date of Next Activity": opp.get("NEXT_ACTIVITY_DATE_UTC"),
                "Probability": opp.get("PROBABILITY"),
                "State Reason": clean_text(state_reason_map.get(str(opp.get("STATE_REASON_ID") or ""), "")),
                "Won": "TRUE" if opp.get("OPPORTUNITY_STATE") == "WON" else "FALSE",
                "Trial?": str(cf.get("Trial__c", False)).upper(),
                "Opportunity Product Quantity": cf.get("Quantity__c", ""),
                "Pricebook Name": clean_text(pricebooks.get(str(opp.get("PRICEBOOK_ID") or ""), "")),
                "Opportunity Owner": clean_text(users.get(str(opp.get("OWNER_USER_ID") or ""), "")),
                "Product Family": clean_text(products.get(pid, "")) if pid else "",
                "Archived Field - Product Type ": clean_text(cf.get("Product_Type__c", "")),
                "Product ID": pid,
                "Organization Name": clean_text(orgs.get(main_org, "")),
                "Owner Name": clean_text(users.get(str(opp.get("OWNER_USER_ID") or ""), "").split(";")[1]
                                         if users.get(str(opp.get("OWNER_USER_ID") or "")) else ""),
                "Channel Type": clean_text(cf.get("Channel_Type__c", "")),
                "GAP Strategy": clean_text(cf.get("GAP_Strategy__c", "")),
                "GAP Current State": clean_text(cf.get("Current_State__c", "")),
                "Invoice Number": invoice_num,
                "Purchase Order": po_number
            }

        if product_ids:
            for pid in product_ids:
                rows.append(base_row(pid))
        else:
            rows.append(base_row(""))

    
    log_time("Built CSV Rows", t0)
    t0 = time.time()
    log_time("Saved CSV File", t0)
 
    

     
    output_file = os.path.join("/tmp", "Opportunities BPR.xlsx")
   

    if rows:
        df = pd.DataFrame(rows)
        df = df.drop_duplicates()
        df.to_excel(output_file, index=False, engine="openpyxl")
        # df.to_csv(output_file, index=False)
        logging.info(f"Exported {len(df)} opportunity rows to {output_file}")
        log_time("Built CSV Rows", t0)
        t0 = time.time()
        log_time("Saved CSV File", t0)
        logging.info(f" Total Execution Time: {round(time.time() - total_start, 2)} seconds")
         
        return output_file
    else:
        logging.warning("No rows to export for opportunities. File will not be created.")
        return None



  