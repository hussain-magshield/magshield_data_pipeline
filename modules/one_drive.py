import os
import base64
import msal
import requests
import yaml
import logging

# ==========================
# 🔐 Load ENV from env.yaml
# ==========================
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
# USERNAME = env.get("USERNAME")

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Files.ReadWrite.All", "Sites.Read.All", "User.Read"]

def get_access_token():
    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}"
    )

    result = app.acquire_token_by_refresh_token(
        REFRESH_TOKEN,
        scopes=SCOPES
    )

    if "access_token" in result:
        logging.info("New access token acquired")
        return result["access_token"]
    else:
        logging.error(f"Failed to acquire access token: {result.get('error_description')}")
        return None

# ==========================
# 🌐 Resolve Shared Link
# ==========================
def get_driveitem_from_share_url(headers, share_url):
    b = base64.b64encode(share_url.encode("utf-8")).decode("utf-8")
    b = b.rstrip("=").replace("/", "_").replace("+", "-")
    share_token = "u!" + b
    endpoint = f"https://graph.microsoft.com/v1.0/shares/{share_token}/driveItem"
    resp = requests.get(endpoint, headers=headers)
    if resp.status_code != 200:
        logging.error(f"Error fetching share: {resp.status_code} | {resp.text}")
        return None
    return resp.json()

def replace_file_on_onedrive(headers, drive_id, item_id, local_file_path):
    """
    Replaces or uploads a file directly to the folder represented by the shared URL.
    """
    file_name = os.path.basename(local_file_path)
    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}:/{file_name}:/content"

    try:
        with open(local_file_path, "rb") as f:
            resp = requests.put(upload_url, headers=headers, data=f)

        if resp.status_code in [200, 201]:
            logging.info(f"Successfully replaced or uploaded: {file_name}")
        else:
            logging.error(f"Failed to replace {file_name}: {resp.status_code} | {resp.text}")

    except FileNotFoundError:
        logging.warning(f"File not found locally: {local_file_path}")
    except Exception as e:
        logging.error(f"Unexpected error replacing file {file_name}: {e}", exc_info=True)

# ==========================
# 🚀 Main Drive Function
# ==========================
def main_drive(share_links, upload_file=None):
    token = get_access_token()
    if not token:
        logging.error("Access token not acquired. Aborting upload.")
        return

    headers = {"Authorization": f"Bearer {token}"}

    for link in share_links:
        logging.info(f"Resolving link: {link}")
        info = get_driveitem_from_share_url(headers, link)
        if info:
            drive_id = info.get("parentReference", {}).get("driveId")
            item_id = info.get("id")
            name = info.get("name")
            logging.info(f"Shared folder resolved: {name} | Drive ID: {drive_id} | Item ID: {item_id}")

            if upload_file:
                replace_file_on_onedrive(headers, drive_id, item_id, upload_file)
        else:
            logging.warning("Could not resolve shared folder from link.")
