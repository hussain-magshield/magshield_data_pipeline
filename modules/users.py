import time
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError
import pandas as pd
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
    for key in ["INSIGHTLY_API_KEY", "CLIENT_ID", "TENANT_ID", "REFRESH_TOKEN"]:
        if os.environ.get(key):
            config[key] = os.environ.get(key)

    return config


env = load_env_config()

API_KEY = env.get("INSIGHTLY_API_KEY")
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
#  Fetch All Users
# ==============================
def fetch_all_users():
    users = []
    skip = 0
    top = 500
    while True:
        params = {
            "brief": "false",
            "skip": skip,
            "top": top,
            "count_total": "true"
        }
        resp = safe_get(f"{BASE_URL}/Users", params=params)
        if not resp:
            break
        data = resp.json()
        if not data:
            break
        users.extend(data)
        logging.info(f"Fetched {len(data)} users (total {len(users)})")

        total_count = int(resp.headers.get("X-Total-Count", len(users)))
        if len(users) >= total_count:
            break
        skip += top
    return users


# ==============================
#  Helper: Date Formatter
# ==============================
 

# ==============================
#  Main Execution
# ==============================
def main_users():
    users = fetch_all_users()

    if not users:
        logging.warning("No users found. Skipping file generation.")
        return None

    rows = []
    for u in users:
        row = {
            "USER_ID": u.get("USER_ID"),
            "CONTACT_ID": u.get("CONTACT_ID"),
            "FIRST_NAME": u.get("FIRST_NAME"),
            "LAST_NAME": u.get("LAST_NAME"),
            "TIMEZONE_ID": u.get("TIMEZONE_ID"),
            "EMAIL_ADDRESS": u.get("EMAIL_ADDRESS"),
            "EMAIL_DROPBOX_IDENTIFIER": u.get("EMAIL_DROPBOX_IDENTIFIER"),
            "EMAIL_DROPBOX_ADDRESS": u.get("EMAIL_DROPBOX_ADDRESS"),
            "ADMINISTRATOR": u.get("ADMINISTRATOR"),
            "ACCOUNT_OWNER": u.get("ACCOUNT_OWNER"),
            "ACTIVE": u.get("ACTIVE"),
            "DATE_CREATED_UTC":  u.get("DATE_CREATED_UTC"),
            "DATE_UPDATED_UTC":  u.get("DATE_UPDATED_UTC"),
            "USER_CURRENCY": u.get("USER_CURRENCY"),
            "CONTACT_DISPLAY": u.get("CONTACT_DISPLAY"),
            "CONTACT_ORDER": u.get("CONTACT_ORDER"),
            "TASK_WEEK_START": u.get("TASK_WEEK_START"),
            "INSTANCE_ID": u.get("INSTANCE_ID"),
            "PROFILE_ID": u.get("PROFILE_ID"),
            "ROLE_ID": u.get("ROLE_ID"),
        }
        rows.append(row)

    output_file = os.path.join("/tmp", "Users.xlsx")
    if rows:
        df = pd.DataFrame(rows)
        df = df.drop_duplicates()
        df.to_excel(output_file, index=False, engine="openpyxl")
        logging.info(f"Exported {len(rows)} users to {output_file}")
        return output_file
    else:
        logging.warning("No rows to export. File will not be created.")
        return None


 
