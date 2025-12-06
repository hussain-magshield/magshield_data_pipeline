import os
import base64
import msal
import requests
import yaml
import logging

 
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
def main_drive(share_links,token, upload_file=None):
    # token = get_access_token()
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
