import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import yaml
import os
import logging

# ==============================
#  Logging
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==============================
#  ENV
# ==============================
def load_env_config(file_path="env.yaml"):
    config = {}
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            config = yaml.safe_load(f) or {}

    if os.environ.get("INSIGHTLY_API_KEY"):
        config["INSIGHTLY_API_KEY"] = os.environ["INSIGHTLY_API_KEY"]

    return config


env = load_env_config()

API_KEY = env.get("INSIGHTLY_API_KEY")
BASE_URL = "https://api.na1.insightly.com/v3.1"
auth = HTTPBasicAuth(API_KEY, "")

# ==============================
#  Safe GET
# ==============================
def safe_get(url, params=None, max_retries=4, timeout=40):
    backoff = 2
    for attempt in range(max_retries):
        try:
            r = requests.get(url, auth=auth, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == max_retries - 1:
                logging.error(f"Failed after retries → {url}")
                return None
            logging.warning(f"Retry {attempt+1}/{max_retries} → {url}")
            time.sleep(backoff ** attempt)

# ==============================
#  PAGED FETCH (ALL TASKS)
# ==============================
def fetch_all(endpoint, top=500):
    records = []
    skip = 0
    while True:
        r = safe_get(f"{BASE_URL}/{endpoint}", params={"skip": skip, "top": top})
        if not r:
            break
        chunk = r.json()
        if not chunk:
            break
        records.extend(chunk)
        skip += top
    return records

# ==============================
#  BULK FETCH FOR SPECIFIC IDs
# (Fetch only linked IDs in batches)
# ==============================
def fetch_by_ids(endpoint, id_field_name, id_list, batch_size=80):
    if not id_list:
        return []

    id_list = list(set(id_list))   # unique
    all_rows = []

    def fetch_batch(batch_ids):
        values = ",".join([str(i) for i in batch_ids])
        url = f"{BASE_URL}/{endpoint}"
        params = {"$filter": f"{id_field_name} in ({values})"}
        r = safe_get(url, params=params)
        return r.json() if r else []

    # batch into chunks
    batches = [id_list[i:i + batch_size] for i in range(0, len(id_list), batch_size)]

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(fetch_batch, b) for b in batches]
        for f in as_completed(futures):
            rows = f.result()
            if rows:
                all_rows.extend(rows)

    logging.info(f"{endpoint}: {len(all_rows)} rows fetched (linked only)")
    return all_rows

# ==============================
#  Format Date
# ==============================
def format_date_only(date_str):
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%m/%d/%Y")
    except:
        return date_str

# ==============================
#  MAIN TASK EXPORT
# ==============================
def main_task():
    logging.info("Starting Task Export...")
    
    logging.info("Warming up Insightly connection...")
    safe_get(f"{BASE_URL}/Users", params={"top": 1})
    time.sleep(2)

    # ---------------------------------------
    # Step 1: Fetch all tasks
    # ---------------------------------------
    tasks = fetch_all("Tasks")
    logging.info(f"Total tasks fetched: {len(tasks)}")

    if not tasks:
        logging.warning("No tasks found.")
        return None

    # ---------------------------------------
    # Step 2: Collect all linked IDs
    # ---------------------------------------
    category_ids = set()
    user_ids = set()
    contact_ids = set()
    lead_ids = set()
    opportunity_ids = set()
    org_ids = set()
    project_ids = set()
    note_ids = set()

    for t in tasks:
        if t.get("CATEGORY_ID"):
            category_ids.add(t["CATEGORY_ID"])

        if t.get("OWNER_USER_ID"):
            user_ids.add(t["OWNER_USER_ID"])

        for link in t.get("LINKS", []):
            obj = link.get("LINK_OBJECT_NAME")
            oid = link.get("LINK_OBJECT_ID")
            if not oid:
                continue

            if obj == "Contact": contact_ids.add(oid)
            elif obj == "Lead": lead_ids.add(oid)
            elif obj == "Opportunity": opportunity_ids.add(oid)
            elif obj == "Organisation": org_ids.add(oid)
            elif obj == "Project": project_ids.add(oid)
            elif obj == "Note": note_ids.add(oid)

    # ---------------------------------------
    # Step 3: BULK FETCH ONLY REQUIRED IDs
    # ---------------------------------------
    all_categories = fetch_by_ids("TaskCategories", "CATEGORY_ID", category_ids)
    all_users = fetch_by_ids("Users", "USER_ID", user_ids)
    all_contacts = fetch_by_ids("Contacts", "CONTACT_ID", contact_ids)
    all_leads = fetch_by_ids("Leads", "LEAD_ID", lead_ids)
    all_opportunities = fetch_by_ids("Opportunities", "OPPORTUNITY_ID", opportunity_ids)
    all_orgs = fetch_by_ids("Organisations", "ORGANISATION_ID", org_ids)
    all_projects = fetch_by_ids("Projects", "PROJECT_ID", project_ids)
    all_notes = fetch_by_ids("Notes", "NOTE_ID", note_ids)

    # ---------------------------------------
    # Step 4: Build lookup maps
    # ---------------------------------------
    category_map = {c["CATEGORY_ID"]: c.get("CATEGORY_NAME", "") for c in all_categories}
    user_map = {
        u["USER_ID"]: f'{u["USER_ID"]};{u.get("FIRST_NAME","")} {u.get("LAST_NAME","")}'
        for u in all_users
    }
    contact_map = {
        c["CONTACT_ID"]: f'{c.get("FIRST_NAME","")} {c.get("LAST_NAME","")}'
        for c in all_contacts
    }
    lead_map = {
        l["LEAD_ID"]: f'{l.get("FIRST_NAME","")} {l.get("LAST_NAME","")}'
        for l in all_leads
    }
    opportunity_map = {
        o["OPPORTUNITY_ID"]: (o.get("OPPORTUNITY_NAME", ""), o.get("ORGANISATION_ID"))
        for o in all_opportunities
    }
    org_map = {
        o["ORGANISATION_ID"]: o.get("ORGANISATION_NAME", "")
        for o in all_orgs
    }
    project_map = {
        p["PROJECT_ID"]: p.get("PROJECT_NAME", "")
        for p in all_projects
    }
    note_map = {
        n["NOTE_ID"]: n.get("TITLE", "")
        for n in all_notes
    }

    # ---------------------------------------
    # Step 5: Transform rows
    # ---------------------------------------
    rows = []

    for t in tasks:
        linked_contact = linked_lead = linked_opp = linked_org = linked_proj = linked_note = ""

        for link in t.get("LINKS", []):
            obj = link.get("LINK_OBJECT_NAME")
            oid = link.get("LINK_OBJECT_ID")

            if obj == "Contact":
                linked_contact = contact_map.get(oid, "")

            elif obj == "Lead":
                linked_lead = lead_map.get(oid, "")

            elif obj == "Opportunity":
                opp_name, org_id = opportunity_map.get(oid, ("", None))
                linked_opp = opp_name
                if org_id:
                    linked_org = org_map.get(org_id, "")

            elif obj == "Organisation":
                linked_org = org_map.get(oid, "")

            elif obj == "Project":
                linked_proj = project_map.get(oid, "")

            elif obj == "Note":
                linked_note = note_map.get(oid, "")

        rows.append({
            "TaskID": t.get("TASK_ID"),
            "Category": category_map.get(t.get("CATEGORY_ID"), ""),
            "Status": t.get("STATUS"),
            "Percent Complete": t.get("PERCENT_COMPLETE"),
            "Priority": t.get("PRIORITY"),
            "Owner Name": user_map.get(t.get("OWNER_USER_ID"), ""),
            "Assigned To Team": t.get("ASSIGNED_TEAM_ID"),
            "Date Assigned": format_date_only(t.get("ASSIGNED_DATE_UTC")),
            "Date Created": format_date_only(t.get("DATE_CREATED_UTC")),
            "Date Reminder": format_date_only(t.get("REMINDER_DATE_UTC")),
            "Date Due": format_date_only(t.get("DUE_DATE")),
            "Date Completed": format_date_only(t.get("COMPLETED_DATE_UTC")),
            "Linked Contact": linked_contact,
            "Linked Lead": linked_lead,
            "Linked Opportunity": linked_opp,
            "Linked Organization": linked_org,
            "Linked Project": linked_proj,
            "Linked Note": linked_note,
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

