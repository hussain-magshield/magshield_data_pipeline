import os
import base64
import msal
import requests
import yaml
import logging

import time
 
_COLD_START = True

import requests

def safe_request(
    method,
    url,
    headers=None,
    data=None,
    params=None,
    max_retries=5,
    timeout=20
):
    last_error = None

    for attempt in range(max_retries):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                data=data,
                params=params,
                timeout=timeout,
                verify=False
            )
            return response

        except requests.exceptions.ConnectionError as e:
            last_error = e
            sleep_time = min(2 ** attempt, 10)

            logging.warning(
                f"Network/DNS error calling {url}. "
                f"Retry {attempt + 1}/{max_retries} in {sleep_time}s"
            )
            time.sleep(sleep_time)

    raise last_error

# def safe_request(
#     method,
#     url,
#     headers=None,
#     data=None,
#     params=None,
#     max_retries=5,
#     timeout=20
# ):
#     """
#     Azure-safe HTTP request with DNS retry & exponential backoff
#     """
#     last_error = None

#     for attempt in range(max_retries):
#         try:
#             resp = requests.request(
#                 method=method,
#                 url=url,
#                 headers=headers,
#                 data=data,
#                 params=params,
#                 timeout=timeout,
#                 verify=False  # SSL already handled earlier
#             )
#             return resp

#         except requests.exceptions.ConnectionError as e:
#             last_error = e
#             sleep_time = 2 ** attempt
#             logging.warning(
#                 f"Network/DNS error calling {url}. "
#                 f"Retry {attempt + 1}/{max_retries} in {sleep_time}s"
#             )
#             time.sleep(sleep_time)

#     raise last_error


# requests.packages.urllib3.disable_warnings(
#     requests.packages.urllib3.exceptions.InsecureRequestWarning
# )
 
def get_driveitem_from_share_url(headers, share_url):
    b = base64.b64encode(share_url.encode("utf-8")).decode("utf-8")
    b = b.rstrip("=").replace("/", "_").replace("+", "-")
    share_token = "u!" + b
    endpoint = f"https://graph.microsoft.com/v1.0/shares/{share_token}/driveItem"
    # resp = requests.get(endpoint, headers=headers,verify=False)
    resp = safe_request("GET", endpoint, headers=headers)

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
            # resp = requests.put(upload_url, headers=headers, data=f,verify=False)
            resp = safe_request("PUT",upload_url,headers=headers, data=f)


        if resp.status_code in [200, 201]:
            logging.info(f"Successfully replaced or uploaded: {file_name}")
        else:
            logging.error(f"Failed to replace {file_name}: {resp.status_code} | {resp.text}")

    except FileNotFoundError:
        logging.warning(f"File not found locally: {local_file_path}")
    except Exception as e:
        logging.error(f"Unexpected error replacing file {file_name}: {e}", exc_info=True)


def replace_existing_file(headers, drive_id, file_item_id, local_file_path):

    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{file_item_id}/content"

    with open(local_file_path, "rb") as f:
        resp = safe_request("PUT", upload_url, headers=headers, data=f)

    if resp.status_code in [200, 201]:
        logging.info("File replaced successfully.")
    else:
        logging.error(f"Replace failed: {resp.status_code} | {resp.text}")
        

def find_file_in_folder(headers, drive_id, folder_item_id, target_filename):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_item_id}/children"

    resp = safe_request("GET", url, headers=headers)

    if resp.status_code != 200:
        logging.error(f"Failed to list folder contents: {resp.status_code} | {resp.text}")
        return None

    items = resp.json().get("value", [])

    for item in items:
        if item.get("name") == target_filename:
            logging.info(f"Found file in folder: {target_filename}")
            return item.get("id")

    logging.warning(f"File {target_filename} not found in folder.")
    return None
# ==========================
# 🚀 Main Drive Function
# ==========================
def main_drive(share_links,token, upload_file=None):
    # token = get_access_token()
    global _COLD_START

    # 🔥 Cold-start DNS warm-up (runs ONCE per instance)
    if _COLD_START:
        logging.info("Cold start detected — warming network (2s)")
        time.sleep(2)
        _COLD_START = False
        
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

            # if upload_file:
            #     replace_file_on_onedrive(headers, drive_id, item_id, upload_file)
            if upload_file:
                file_name = os.path.basename(upload_file)

                # 1️⃣ Find file inside folder
                file_id = find_file_in_folder(headers, drive_id, item_id, file_name)

                if file_id:
                    # 2️⃣ Replace existing file
                    replace_existing_file(headers, drive_id, file_id, upload_file)
                else:
                    logging.warning("File not found in folder. Uploading as new file.")

                    # Optional: upload new if not found
                    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}:/{file_name}:/content"

                    with open(upload_file, "rb") as f:
                        resp = safe_request("PUT", upload_url, headers=headers, data=f)

                    logging.info(f"Upload status: {resp.status_code}")
        else:
            logging.warning("Could not resolve shared folder from link.")
