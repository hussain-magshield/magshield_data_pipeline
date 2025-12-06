import requests
import msal 
import pandas as pd
import os
import zipfile
import io
from datetime import datetime, timedelta
from bs4 import BeautifulSoup 
from urllib.parse import urlparse
import os
import yaml
import logging

# ==============================
#  Logging Config
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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

 
CLIENT_ID = env.get("CLIENT_ID")
TENANT_ID = env.get("TENANT_ID")
REFRESH_TOKEN = env.get("REFRESH_TOKEN")
 

# Graph API settings
SCOPE = ['Mail.Read', 'User.Read'] # Corrected scope (offline_access removed)
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
INSIGHTLY_SENDER = 'notifications@insightly.com' 
SUBJECT_FILTER = 'Insightly has finished exporting your report:' 
OUTPUT_DIR = "temp" 
RENAMED_FILE = "Opp Stage Duration.xlsx"
 

 

# def get_access_token_via_refresh_token(refresh_token):
#     """
#     Refresh token ka use karke naya access token acquire karta hai.
#     SSL error ko handle karne ke liye session.verify=False use kiya gaya hai.
#     """
    
   
#     session = requests.Session()
     
#     requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
#     session.verify = False 

    
#     app = msal.PublicClientApplication(
#         CLIENT_ID, 
#         authority=AUTHORITY,
#         http_client=session   
#     ) 
    
#     logging.info("Acquiring new access token using refresh token...")
    
#     try:
#         result = app.acquire_token_by_refresh_token(
#             refresh_token,
#             scopes=SCOPE 
#         )
#     except Exception as e:
#         logging.critical(f"MSAL Exception during token acquisition: {e}")
#         raise

#     if 'access_token' in result:
#         # Naya: Token ke saath session object bhi return karein
#         return result['access_token'], session 
#     else:
#         logging.error(f"Authentication failed: {result.get('error_description', 'Unknown error')}")
#         raise Exception("Authentication failed.")

 
def process_zip_data(file_content, original_filename, renamed_file_name):
    """
    Downloaded content ko temp folder mein save karta hai, rename karta hai, aur uska path return karta hai.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    
    initial_path = os.path.join(OUTPUT_DIR, original_filename)
    
    try:
        with open(initial_path, 'wb') as f:
            f.write(file_content)
        
        logging.info(f"Report downloaded successfully to: {initial_path}")
        
        if original_filename.lower().endswith(".xlsx"):
            final_path = os.path.join(OUTPUT_DIR, f"{renamed_file_name}.xlsx")

            if os.path.exists(final_path):
                os.remove(final_path)

            os.rename(initial_path, final_path)
            return final_path
        
        if original_filename.lower().endswith(".csv"):

            # Final Excel path (renamed_file_name should end with .xlsx)
            final_path = os.path.join(OUTPUT_DIR, renamed_file_name)

            # Remove existing Excel file if exists
            if os.path.exists(final_path):
                os.remove(final_path)

            # --- NEW: Convert CSV → Excel ---
            try:
                import pandas as pd
                df = pd.read_csv(initial_path)     # read CSV
                df.to_excel(final_path, index=False)   # write Excel

                logging.info(f"CSV converted to Excel: {final_path}")
            except Exception as e:
                logging.error(f"Error converting CSV to Excel: {e}")
                return None
            finally:
                # delete original CSV
                try:
                    os.remove(initial_path)
                except:
                    pass

            return final_path
         
        
    except Exception as e:
        logging.error(f"Error saving or renaming file: {e}")
        return None


 
    
def extract_download_link(token, message_id, session):
    """
    Email body se Download Report link aur original filename extract karta hai (Using session).
    """
    headers = {'Authorization': f'Bearer {token}'}
    body_url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}?$select=body"
    
    try:
        # FIXED: session.get ka use
        response = session.get(body_url, headers=headers) 
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error during fetching body: {e}")
        return None, None

    body_data = response.json().get('body', {})
    html_content = body_data.get('content', '')
    
    soup = BeautifulSoup(html_content, 'html.parser')
    link_tag = soup.find('a', string=lambda t: t and 'Download Report' in t)
    
    if link_tag and 'href' in link_tag.attrs:
        download_link = link_tag['href']
        logging.info(f"Extracted Download Link: {download_link}")
        
        parsed_url = urlparse(download_link)
        path = parsed_url.path
        original_filename = path.split('/')[-1]
        
        return download_link, original_filename 
    else:
        logging.warning("Error: 'Download Report' link not found in email body.")
        return None, None
    
    
 

def download_report_from_link(download_url, original_filename, renamed_file_name, session):
    """
    Extracted link ka use karke file download karta hai (Using session).
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Accept': 'application/json, text/plain, */*'
    }
    
    try:
         
        download_response = session.get(download_url, stream=True, headers=headers) 
        download_response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error during download (Check link expiry/403 Forbidden): {e}")
        return None
    
    final_path = process_zip_data(download_response.content, original_filename, renamed_file_name)
    
    return final_path
    
     
    
    
    
def download_insightly_report(token,session):
    
    
    headers = {'Authorization': f'Bearer {token}'}
    
     
    last_date_to_check = datetime.now() - timedelta(days=15) 
    filter_date_clean = last_date_to_check.replace(microsecond=0).isoformat() + 'Z'
    
    
    filter_string = (
        f"receivedDateTime ge {filter_date_clean} and "
        f"sender/emailAddress/address eq '{INSIGHTLY_SENDER}'"
    )
    
    search_url = (
        f"https://graph.microsoft.com/v1.0/me/messages?"
        f"$filter={filter_string}"
        f"&$orderby=receivedDateTime desc" 
        f"&$top=10" 
    )
    
   
    logging.info(f"Searching URL: {search_url}")
    
     
    
    try:
        
        response = session.get(search_url, headers=headers) 
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error during search: {e}")
        return


    messages = response.json().get('value', [])
     
    logging.info(f"Messages found: {len(messages)}")
    
    
    if not messages:
        logging.info("No new Insightly report email found in the last 10 days matching the sender.")
        
        return

     
    TARGET_REPORT_NAME = 'Insightly - Opportunity Stage Duration Export'
    
     
    target_report_found = None
    logging.info(f"\nFiltering for report: '{TARGET_REPORT_NAME}'")
    
    

    for message in messages:
        subject = message.get('subject', '')
        
        
        if TARGET_REPORT_NAME in subject:
             target_report_found = message
             logging.info(f"SUCCESS: Found target report in subject: {subject}")
             
             break
        
        

    if not target_report_found:
        logging.info(f"ERROR: Report '{TARGET_REPORT_NAME}' not found in the fetched emails.")
        
        return
 
    message_id = target_report_found['id']
    logging.info(f"Final Message ID for download: {message_id}")
    logging.info(f"message: {target_report_found}")
      
   
    download_link, filename = extract_download_link(token, message_id,session)
    renamed_file = RENAMED_FILE  
    
    final_report_path = None
    
    if download_link:
        final_report_path = download_report_from_link(download_link, filename, renamed_file,session)
    
        
    else:
        logging.info("Download process terminated as link could not be extracted.")
       
        
        
    return final_report_path    

 
def main_opp_stage(access_token, session):
    
    # access_token, session = get_access_token_via_refresh_token(REFRESH_TOKEN)
    data=download_insightly_report(access_token,session)     
    logging.info(f"Final downloaded report path: {data}") 
    return data  
    