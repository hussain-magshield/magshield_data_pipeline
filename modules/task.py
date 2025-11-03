# ==============================
#  TASK EXPORT SCRIPT (PARALLEL OPTIMIZED)
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
MAX_WORKERS = 30

# ==============================
#  Safe GET with Backoff
# ==============================
def safe_get(url, params=None, max_retries=3, timeout=30):
    backoff = 2
    for attempt in range(max_retries):
        try:
            r = requests.get(url, auth=auth, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except (ChunkedEncodingError, ConnectionError, Timeout) as e:
            wait_time = backoff ** attempt
            logging.warning(f"Network error: {e} | Attempt {attempt+1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(wait_time)
            else:
                logging.error(f"Skipping: {url}")
                return None
        except requests.HTTPError as e:
            logging.error(f"HTTP error: {e} | URL: {url}")
            return None

# ==============================
#  Caches
# ==============================
category_cache = {}
user_cache = {}
contact_cache = {}
lead_cache = {}
opportunity_cache = {}
organization_cache = {}
project_cache = {}
note_cache = {}

# ==============================
#  Lookup functions
# ==============================
def fetch_category(cid):
    if cid and cid not in category_cache:
        r = safe_get(f"{BASE_URL}/TaskCategories/{cid}")
        if r:
            category_cache[cid] = r.json().get("CATEGORY_NAME", "")

def fetch_user(uid):
    uid = str(uid)
    if uid and uid not in user_cache:
        r = safe_get(f"{BASE_URL}/Users/{uid}")
        if r:
            u = r.json()
            user_cache[uid] = f'{u.get("USER_ID")};{u.get("FIRST_NAME","")} {u.get("LAST_NAME","")}'.strip()

def fetch_contact(cid):
    cid = str(cid)
    if cid and cid not in contact_cache:
        r = safe_get(f"{BASE_URL}/Contacts/{cid}")
        if r:
            c = r.json()
            contact_cache[cid] = f'{c.get("FIRST_NAME","")} {c.get("LAST_NAME","")}'.strip()

def fetch_lead(lid):
    lid = str(lid)
    if lid and lid not in lead_cache:
        r = safe_get(f"{BASE_URL}/Leads/{lid}")
        if r:
            l = r.json()
            lead_cache[lid] = f'{l.get("FIRST_NAME","")} {l.get("LAST_NAME","")}'.strip()

def fetch_opportunity(oid):
    oid = str(oid)
    if oid and oid not in opportunity_cache:
        r = safe_get(f"{BASE_URL}/Opportunities/{oid}")
        if r:
            o = r.json()
            opportunity_cache[oid] = (o.get("OPPORTUNITY_NAME", ""), o.get("ORGANISATION_ID"))

def fetch_organization(oid):
    oid = str(oid)
    if oid and oid not in organization_cache:
        r = safe_get(f"{BASE_URL}/Organisations/{oid}")
        if r:
            organization_cache[oid] = r.json().get("ORGANISATION_NAME", "")

def fetch_project(pid):
    pid = str(pid)
    if pid and pid not in project_cache:
        r = safe_get(f"{BASE_URL}/Projects/{pid}")
        if r:
            project_cache[pid] = r.json().get("PROJECT_NAME", "")

def fetch_note(nid):
    nid = str(nid)
    if nid and nid not in note_cache:
        r = safe_get(f"{BASE_URL}/Notes/{nid}")
        if r:
            note_cache[nid] = r.json().get("TITLE", "")

# ==============================
#  Fetch All Tasks
# ==============================
def fetch_all_tasks():
    tasks = []
    skip = 0
    top = 500
    while True:
        params = {"skip": skip, "top": top}
        resp = safe_get(f"{BASE_URL}/Tasks", params=params)
        if not resp:
            break
        data = resp.json()
        if not data:
            break
        tasks.extend(data)
        skip += top
        logging.info(f"{len(tasks)} tasks fetched so far")
    return tasks

# ==============================
#  Parallel Lookup
# ==============================
def parallel_lookups(cat_ids, user_ids, contact_ids, lead_ids, opp_ids, org_ids, proj_ids, note_ids):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for cid in cat_ids: futures.append(executor.submit(fetch_category, cid))
        for uid in user_ids: futures.append(executor.submit(fetch_user, uid))
        for cid in contact_ids: futures.append(executor.submit(fetch_contact, cid))
        for lid in lead_ids: futures.append(executor.submit(fetch_lead, lid))
        for oid in opp_ids: futures.append(executor.submit(fetch_opportunity, oid))
        for oid in org_ids: futures.append(executor.submit(fetch_organization, oid))
        for pid in proj_ids: futures.append(executor.submit(fetch_project, pid))
        for nid in note_ids: futures.append(executor.submit(fetch_note, nid))

        for _ in as_completed(futures):
            pass
    logging.info(f"Lookups done → Categories={len(category_cache)}, Users={len(user_cache)}, Contacts={len(contact_cache)}")

# ==============================
#  Main Function
# ==============================
from datetime import datetime

def format_date_only(date_str):
    """Convert 'YYYY-MM-DD HH:MM:SS' → 'MM/DD/YYYY'"""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%m/%d/%Y")
    except ValueError:
        return date_str  # 
    
    
def main_task():
    logging.info("Starting Task Export...")
    tasks = fetch_all_tasks()

    if not tasks:
        logging.warning("No tasks found. Skipping file generation.")
        return None

    # Collect unique IDs
    cat_ids, user_ids, contact_ids, lead_ids, opp_ids, org_ids, proj_ids, note_ids = set(), set(), set(), set(), set(), set(), set(), set()
    for t in tasks:
        if t.get("CATEGORY_ID"): cat_ids.add(t["CATEGORY_ID"])
        if t.get("OWNER_USER_ID"): user_ids.add(t["OWNER_USER_ID"])
        for link in t.get("LINKS", []):
            obj, obj_id = link.get("LINK_OBJECT_NAME"), link.get("LINK_OBJECT_ID")
            if obj == "Contact": contact_ids.add(obj_id)
            elif obj == "Lead": lead_ids.add(obj_id)
            elif obj == "Opportunity": opp_ids.add(obj_id)
            elif obj == "Organisation": org_ids.add(obj_id)
            elif obj == "Project": proj_ids.add(obj_id)
            elif obj == "Note": note_ids.add(obj_id)

    # Parallel fetching
    parallel_lookups(cat_ids, user_ids, contact_ids, lead_ids, opp_ids, org_ids, proj_ids, note_ids)

    # Transform data
    rows = []
    for t in tasks:
        linked_contact = linked_lead = linked_opportunity = linked_org = linked_project = linked_note = ""
        for link in t.get("LINKS", []):
            obj, obj_id = link.get("LINK_OBJECT_NAME"), str(link.get("LINK_OBJECT_ID"))
            if obj == "Contact":
                linked_contact = contact_cache.get(obj_id, "")
            elif obj == "Lead":
                linked_lead = lead_cache.get(obj_id, "")
            elif obj == "Opportunity":
                opp_name, org_id = opportunity_cache.get(obj_id, ("", None))
                linked_opportunity = opp_name
                if org_id:
                    linked_org = organization_cache.get(str(org_id), "")
            elif obj == "Organisation":
                linked_org = organization_cache.get(obj_id, "")
            elif obj == "Project":
                linked_project = project_cache.get(obj_id, "")
            elif obj == "Note":
                linked_note = note_cache.get(obj_id, "")

        rows.append({
            "TaskID": t.get("TASK_ID"),
            "Category": category_cache.get(t.get("CATEGORY_ID"), ""),
            "Status": t.get("STATUS"),
            "Percent Complete": t.get("PERCENT_COMPLETE"),
            "Priority": t.get("PRIORITY"),
            "Owner Name": user_cache.get(str(t.get("OWNER_USER_ID")), ""),
            "Assigned To Team": t.get("ASSIGNED_TEAM_ID"),
            "Date Assigned": format_date_only(t.get("ASSIGNED_DATE_UTC")),
            "Date Created": format_date_only(t.get("DATE_CREATED_UTC")),
            "Date Reminder": format_date_only(t.get("REMINDER_DATE_UTC")),
            "Date Due": format_date_only(t.get("DUE_DATE")),
            "Date Completed": format_date_only(t.get("COMPLETED_DATE_UTC")),
            "Linked Contact": linked_contact,
            "Linked Lead": linked_lead,
            "Linked Opportunity": linked_opportunity,
            "Linked Organization": linked_org,
            "Linked Project": linked_project,
            "Linked Note": linked_note
        })

     
    output_file = os.path.join("/tmp", "Tasks.xlsx")
    if rows:
        df = pd.DataFrame(rows)
        df = df.drop_duplicates()

        df.to_excel(output_file, index=False, engine="openpyxl")
        logging.info(f"Exported {len(rows)} tasks to {output_file}")
        return output_file
    else:
        logging.warning("No rows to export for tasks. File will not be created.")
        return None
