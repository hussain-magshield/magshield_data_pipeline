import requests
import pandas as pd
import os
import base64
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote
import io
# ==============================
# CONFIG
# ==============================

MAILBOX = "hussainm@magshield.com"   # <-- CHANGE if needed
INSIGHTLY_SENDER = "notifications@insightly.com"
TARGET_REPORT_NAME = "Insightly - Opportunity Stage Duration Export"
RENAMED_FILE = "Opp Stage Duration.xlsx"
# OUTPUT_DIR = "/tmp"
OUTPUT_DIR = "modules/tmp"
# ==============================
# HELPERS
# ==============================

# def process_file(file_content, original_filename):
#     if not os.path.exists(OUTPUT_DIR):
#         os.makedirs(OUTPUT_DIR)

#     temp_path = os.path.join(OUTPUT_DIR, original_filename)

#     with open(temp_path, "wb") as f:
#         f.write(file_content)

#     final_path = os.path.join(OUTPUT_DIR, RENAMED_FILE)

#     if original_filename.lower().endswith(".csv"):
#         df = pd.read_csv(temp_path)
#         # df.to_excel(final_path, index=False)
#         df.to_excel(final_path, index=False, engine="openpyxl")
#         os.remove(temp_path)
#     else:
#         if os.path.exists(final_path):
#             os.remove(final_path)
#         os.rename(temp_path, final_path)

#     logging.info(f"Saved report: {final_path}")
#     return final_path



def process_file(file_content, original_filename):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    final_path = os.path.join(OUTPUT_DIR, RENAMED_FILE)

    # Try reading as CSV first
    try:
        decoded = file_content.decode("utf-8")
        if decoded.startswith('"') or "," in decoded[:200]:
            df = pd.read_csv(io.StringIO(decoded))
            df.to_excel(final_path, index=False, engine="openpyxl")
            logging.info("CSV detected and converted to Excel.")
            logging.info(f"Saved report: {final_path}")
            return final_path
    except Exception:
        pass

    # If not CSV, try Excel
    try:
        temp_path = os.path.join(OUTPUT_DIR, original_filename)
        with open(temp_path, "wb") as f:
            f.write(file_content)

        df = pd.read_excel(temp_path)
        df.to_excel(final_path, index=False, engine="openpyxl")
        os.remove(temp_path)

        logging.info("Excel file validated and resaved.")
        logging.info(f"Saved report: {final_path}")
        return final_path
    except Exception:
        raise Exception("Downloaded file is neither valid CSV nor valid Excel.")
 

def extract_download_link(token, message_id, session):
    headers = {"Authorization": f"Bearer {token}"}

    body_url = f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/messages/{message_id}?$select=body"

    resp = session.get(body_url, headers=headers, verify=False)
    resp.raise_for_status()

    html = resp.json()["body"]["content"]
    soup = BeautifulSoup(html, "html.parser")

    link = soup.find("a", string=lambda t: t and "Download Report" in t)

    if not link:
        return None, None

    download_url = link["href"]

    parsed = urlparse(download_url)
    params = parse_qs(parsed.query)

    if "url" in params:
        real = unquote(params["url"][0])
        filename = os.path.basename(urlparse(real).path)
    else:
        filename = os.path.basename(parsed.path)

    if not filename:
        filename = "insightly_report.csv"

    return download_url, filename


def download_from_link(url, filename, session):
    r = session.get(url, stream=True, verify=False)
    r.raise_for_status()
    print("Content-Type:", r.headers.get("Content-Type"))
    print("First 200 chars:", r.text[:200])

    return process_file(r.content, filename)


# ==============================
# MAIN WORKFLOW
# ==============================

def download_insightly_report(token, session):

    headers = {"Authorization": f"Bearer {token}"}

    since = (datetime.utcnow() - timedelta(days=15)).replace(microsecond=0).isoformat() + "Z"

    search_url = (
        f"https://graph.microsoft.com/v1.0/users/{MAILBOX}/messages"
        f"?$filter=receivedDateTime ge {since} and sender/emailAddress/address eq '{INSIGHTLY_SENDER}'"
        f"&$orderby=receivedDateTime desc"
        f"&$top=5"
    )

    logging.info(f"Searching URL: {search_url}")

    resp = session.get(search_url, headers=headers, verify=False)
    resp.raise_for_status()

    messages = resp.json().get("value", [])

    if not messages:
        logging.warning("No Insightly emails found.")
        return None

    target = None
    for m in messages:
        if TARGET_REPORT_NAME in m.get("subject", ""):
            target = m
            break

    if not target:
        logging.warning("Target report email not found.")
        return None

    message_id = target["id"]

    download_link, filename = extract_download_link(token, message_id, session)

    if not download_link:
        logging.error("Download link not found.")
        return None

    return download_from_link(download_link, filename, session)


def main_opp_stage(access_token, session):
    path = download_insightly_report(access_token, session)
    logging.info(f"Final downloaded report path: {path}")
    return path
    