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
#  Logging (optional but useful)
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==============================
#  API Configuration
# ==============================

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
#  Helper: Safe GET
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
                logging.error(f"Skipping {url} after {max_retries} failed attempts: {url}")
                return None
        except requests.HTTPError as e:
            logging.error(f"HTTP error on {url}: {e}")
            return None
    return None

# ==============================
#  Fetch paged records in parallel
# ==============================
def fetch_all_paged(endpoint, top=500):
    records = []
    resp = safe_get(f"{BASE_URL}/{endpoint}", params={"top": 1, "count_total": "true"})
    if not resp:
        return records
    total_count = int(resp.headers.get("X-Total-Count", 0))
    total_pages = (total_count // top) + (1 if total_count % top != 0 else 0)
    logging.info(f"{endpoint}: {total_count} records, {total_pages} pages")

    def fetch_page(page_idx):
        skip = page_idx * top
        r = safe_get(f"{BASE_URL}/{endpoint}", params={"skip": skip, "top": top})
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
#  Build Lookup Tables
# ==============================
def build_lookups():
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            "orgs": ex.submit(fetch_all_paged, "Organisations"),
            "users": ex.submit(fetch_all_paged, "Users"),
            "pricebook": ex.submit(fetch_all_paged, "Pricebook"),
            "product": ex.submit(fetch_all_paged, "Product"),
            "state_reason": ex.submit(fetch_all_paged, "OpportunityStateReasons"),
        }
        results = {k: v.result() for k, v in futures.items()}

    # Organisations
    orgs = {
        str(o["ORGANISATION_ID"]): o.get("ORGANISATION_NAME", "")
        for o in results["orgs"]
    }

    # Users
    users = {
        str(u["USER_ID"]): f'{u.get("USER_ID")};{u.get("FIRST_NAME","")} {u.get("LAST_NAME","")}'
        for u in results["users"]
    }

    # Pricebooks
    pricebooks = {
        str(p["PRICEBOOK_ID"]): p.get("NAME", "")
        for p in results["pricebook"]
    }

    # Products
    products_family = {}
    product_codes = {}  # PRODUCT_ID -> PRODUCT_CODE / PRODUCT_SKU

    for p in results["product"]:
        pid = str(p["PRODUCT_ID"])
        products_family[pid] = p.get("PRODUCT_FAMILY", "")
        # Prefer PRODUCT_CODE, fallback to PRODUCT_SKU
        product_code = p.get("PRODUCT_CODE") or p.get("PRODUCT_SKU") or ""
        product_codes[pid] = str(product_code) if product_code is not None else ""

    # State Reasons
    state_reason_map = {
        str(r["STATE_REASON_ID"]): r.get("STATE_REASON", "")
        for r in results["state_reason"]
    }

    return orgs, users, pricebooks, products_family, product_codes, state_reason_map

# ==============================
#  Pricebook Entry Cache
# ==============================
pricebook_entry_cache = {}
def get_product_id_from_pricebook_entry(entry_id):
    if not entry_id:
        return None
    entry_id = str(entry_id)
    if entry_id in pricebook_entry_cache:
        return pricebook_entry_cache[entry_id]
    r = safe_get(f"{BASE_URL}/PricebookEntry/{entry_id}")
    if r and r.status_code == 200:
        pid = r.json().get("PRODUCT_ID")
        if pid:
            pid_str = str(pid)
            pricebook_entry_cache[entry_id] = pid_str
            return pid_str
    return None

# ==============================
#  Build Opportunity Product Maps
# ==============================
def build_opp_product_maps(product_codes):
    """
    Returns:
      opp_product_ids   : { OPPORTUNITY_ID: [PRODUCT_ID, ...] }
      opp_product_codes : { OPPORTUNITY_ID: [PRODUCT_CODE/SKU, ...] }
    """
    opp_product_ids = {}
    opp_product_codes = {}

    line_items = fetch_all_paged("OpportunityLineItem")

    def process_line_item(li):
        pricebook_entry_id = li.get("PRICEBOOK_ENTRY_ID")
        opp_id = li.get("OPPORTUNITY_ID")
        if not opp_id or not pricebook_entry_id:
            return None
        opp_id_str = str(opp_id)
        prod_id = get_product_id_from_pricebook_entry(pricebook_entry_id)
        if not prod_id:
            return None

        # Add PRODUCT_ID
        opp_product_ids.setdefault(opp_id_str, set()).add(prod_id)

        # Add PRODUCT_CODE / SKU
        sku = product_codes.get(prod_id, "")
        if sku:
            opp_product_codes.setdefault(opp_id_str, set()).add(sku)

        return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(process_line_item, li) for li in line_items]
        for _ in as_completed(futures):
            pass

    # convert sets to lists
    opp_product_ids = {k: list(v) for k, v in opp_product_ids.items()}
    opp_product_codes = {k: list(v) for k, v in opp_product_codes.items()}

    logging.info(f"Built product maps for {len(opp_product_ids)} opportunities")
    return opp_product_ids, opp_product_codes

# ==============================
#  Site name and stage lookup
# ==============================
stage_cache = {}
def fetch_stage_name(stage_id):
    if not stage_id:
        return ""
    stage_id = str(stage_id)
    if stage_id in stage_cache:
        return stage_cache[stage_id]
    r = safe_get(f"{BASE_URL}/PipelineStages/{stage_id}")
    if r and r.status_code == 200:
        name = r.json().get("STAGE_NAME", "") or ""
        stage_cache[stage_id] = name
        return name
    return ""

def fetch_site_name(opportunity_id, main_org_id, orgs):
    url = f"{BASE_URL}/Opportunities/{opportunity_id}/Links"
    r = safe_get(url)
    if not r:
        return ""
    links = r.json()
    site_names = [
        orgs.get(str(l.get("LINK_OBJECT_ID")))
        for l in links
        if l.get("LINK_OBJECT_NAME") == "Organisation"
        and str(l.get("LINK_OBJECT_ID")) != str(main_org_id)
    ]
    return " and ".join([n for n in site_names if n])

# ==============================
#  Utility
# ==============================
def clean_text(v):
    return v.replace("\r", " ").replace("\n", " ").strip() if isinstance(v, str) else v

# ==============================
#  Invoice lookup by SKU (Invoiced_Item__c)
# ==============================
def build_invoice_lookup_by_sku():
    """
    Maps SKU → list of invoice rows.
    SKU = Invoiced_Item__c
    """
    invoices = fetch_all_paged("Invoice_History__c")
    lookup = {}

    for inv in invoices:
        cf = {c["FIELD_NAME"]: c.get("FIELD_VALUE") for c in inv.get("CUSTOMFIELDS", [])}
        sku = str(cf.get("Invoiced_Item__c") or "").strip()
        if not sku:
            continue

        row = {
            "Invoice_Num__c": cf.get("Invoice_Num__c", ""),
            "PO_Number__c": cf.get("PO_Number__c", "")
        }

        lookup.setdefault(sku, []).append(row)

    logging.info(f"Built invoice lookup for {len(lookup)} SKUs")
    return lookup

# ==============================
#  Main Execution Function
# ==============================
def main_opportunity():
    # Lookups
    organisations, users, pricebooks, products_family, product_codes, state_reason_map = build_lookups()
    invoice_lookup = build_invoice_lookup_by_sku()
    opp_product_ids_map, opp_product_skus_map = build_opp_product_maps(product_codes)
    opportunities = fetch_all_paged("Opportunities")

    if not opportunities:
        logging.warning("No opportunities found. Skipping file generation.")
        return None

    # Precompute site names & stage names in parallel
    site_name_map = {}
    stage_name_map = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        site_futures = {
            ex.submit(
                fetch_site_name,
                opp.get("OPPORTUNITY_ID"),
                opp.get("ORGANISATION_ID"),
                organisations
            ): opp
            for opp in opportunities
        }
        stage_futures = {
            ex.submit(fetch_stage_name, opp.get("STAGE_ID")): opp
            for opp in opportunities
        }

        for f in as_completed(site_futures):
            opp = site_futures[f]
            site_name_map[str(opp.get("OPPORTUNITY_ID"))] = f.result() or ""

        for f in as_completed(stage_futures):
            opp = stage_futures[f]
            stage_id = str(stage_futures[f].get("STAGE_ID") or "")
            stage_name_map[stage_id] = f.result() or ""

    rows = []

    for opp in opportunities:
        cf = {c["FIELD_NAME"]: c.get("FIELD_VALUE") for c in opp.get("CUSTOMFIELDS", [])}
        org_id = str(opp.get("ORGANISATION_ID") or "")
        entity_org_id = str(cf.get("Entity_Owning_Equipment__c") or "")
        channel_partner_id = str(cf.get("Channel_Owner__c") or "")
        pricebook_id = str(opp.get("PRICEBOOK_ID") or "")
        owner_id = str(opp.get("OWNER_USER_ID") or "")
        stage_id = str(opp.get("STAGE_ID") or "")
        opp_id_str = str(opp.get("OPPORTUNITY_ID"))

        site_name = site_name_map.get(opp_id_str, "")
        current_stage = stage_name_map.get(stage_id, "")
        product_ids = opp_product_ids_map.get(opp_id_str, [])
        state_reason = state_reason_map.get(str(opp.get("STATE_REASON_ID") or ""), "")

        # SKUs for this opportunity
        product_skus = opp_product_skus_map.get(opp_id_str, [])

        # ==============================
        # INVOICE MATCHING (BY SKU)
        # ==============================
        invoice_list = []
        for sku in product_skus:
            sku = str(sku).strip()
            if sku in invoice_lookup:
                invoice_list.extend(invoice_lookup.get(sku, []))

        # Remove duplicates (same invoice_num + PO)
        unique_invoices = []
        seen_pairs = set()
        for inv in invoice_list:
            key = (inv.get("Invoice_Num__c"), inv.get("PO_Number__c"))
            if key not in seen_pairs:
                seen_pairs.add(key)
                unique_invoices.append(inv)

        invoice_list = unique_invoices

        # ==============================
        # Row builder
        # ==============================
        def base_row(pid="", invoice_num="", po_number=""):
            return {
                "Opportunity ID": opp_id_str,
                "Opportunity Name": clean_text(opp.get("OPPORTUNITY_NAME", "")),
                "Entity Owning Equipment": clean_text(organisations.get(entity_org_id, "")),
                "Site Name": clean_text(site_name),
                "Channel Partner": clean_text(organisations.get(channel_partner_id, "")),
                "Date Created": opp.get("DATE_CREATED_UTC"),
                "Date Closed (Forecast)": opp.get("FORECAST_CLOSE_DATE"),
                "Date Closed (Actual)": opp.get("ACTUAL_CLOSE_DATE"),
                "Opportunity Value": opp.get("OPPORTUNITY_VALUE"),
                "Bid Currency": opp.get("BID_CURRENCY"),
                "Opportunity State": opp.get("OPPORTUNITY_STATE"),
                "Current Pipeline Stage": clean_text(current_stage),
                "Expected Revenue": opp.get("OPPORTUNITY_VALUE"),
                "Date of Last Activity": opp.get("LAST_ACTIVITY_DATE_UTC"),
                "Date of Next Activity": opp.get("NEXT_ACTIVITY_DATE_UTC"),
                "Probability": opp.get("PROBABILITY"),
                "State Reason": clean_text(state_reason),
                "Won": "TRUE" if opp.get("OPPORTUNITY_STATE") == "WON" else "FALSE",
                "Trial?": str(cf.get("Trial__c", False)).upper(),
                "Opportunity Product Quantity": cf.get("Quantity__c", ""),
                "Pricebook Name": clean_text(pricebooks.get(pricebook_id, "")),
                "Opportunity Owner": clean_text(users.get(owner_id, "")),
                "Product Family": clean_text(products_family.get(pid, "")) if pid else "",
                "Archived Field - Product Type ": clean_text(cf.get("Product_Type__c", "")),
                "Product ID": pid,
                "Organization Name": clean_text(organisations.get(org_id, "")),
                "Owner Name": clean_text(users.get(owner_id, "").split(";")[1] if users.get(owner_id) else ""),
                "Channel Type": clean_text(cf.get("Channel_Type__c", "")),
                "GAP Strategy": clean_text(cf.get("GAP_Strategy__c", "")),
                "GAP Current State": clean_text(cf.get("Current_State__c", "")),
                "Invoice Number": clean_text(invoice_num),
                "Purchase Order": clean_text(po_number),
            }

        # ==============================
        # Build rows
        # ==============================
        if invoice_list:
            # For each invoice, attach all products (or blank if none)
            for inv in invoice_list:
                inv_num = inv.get("Invoice_Num__c", "")
                po_num = inv.get("PO_Number__c", "")
                if product_ids:
                    for pid in product_ids:
                        rows.append(base_row(pid, inv_num, po_num))
                else:
                    rows.append(base_row("", inv_num, po_num))
        else:
            # No invoice: still one row (per product, or single if no product)
            if product_ids:
                for pid in product_ids:
                    rows.append(base_row(pid, "", ""))
            else:
                rows.append(base_row("", "", ""))

    # ==============================
    # Export
    # ==============================
    output_file = os.path.join("/tmp", "Opportunities BPR.xlsx")

    if rows:
        df = pd.DataFrame(rows)
        df = df.drop_duplicates()
        df.to_excel(output_file, index=False, engine="openpyxl")
        logging.info(f"Exported {len(df)} opportunity rows to {output_file}")
        return output_file
    else:
        logging.warning("No rows to export for opportunities. File will not be created.")
        return None
