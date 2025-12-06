import time
import os
import logging
import base64
import msal
import requests
import yaml
from modules.one_drive import main_drive
from modules.quote import main_quote
from modules.task import main_task
from modules.organisation import main_organisation
from modules.opportunity import main_opportunity
from modules.equiment import main_equipment_export
from modules.invoice import main_invoice_export
from modules.users import main_users
from modules.opportunity_stage import main_opp_stage    

 

 

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
 

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Files.ReadWrite.All", "Sites.Read.All", "User.Read"]

ACCESS_TOKEN = None
SESSION = None




def get_access_token_via_refresh_token(refresh_token):
    """
    Refresh token ka use karke naya access token acquire karta hai.
    SSL error ko handle karne ke liye session.verify=False use kiya gaya hai.
    """
    
   
    session = requests.Session()
     
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    session.verify = False 

    
    app = msal.PublicClientApplication(
        CLIENT_ID, 
        authority=AUTHORITY,
        http_client=session   
    ) 
    
    logging.info("Acquiring new access token using refresh token...")
    
    try:
        result = app.acquire_token_by_refresh_token(
            refresh_token,
            scopes=SCOPES 
        )
    except Exception as e:
        logging.critical(f"MSAL Exception during token acquisition: {e}")
        raise

    if 'access_token' in result:
        # Naya: Token ke saath session object bhi return karein
        return result['access_token'], session 
    else:
        logging.error(f"Authentication failed: {result.get('error_description', 'Unknown error')}")
        raise Exception("Authentication failed.")

 


def init_token_once():
    global ACCESS_TOKEN, SESSION
    
    if ACCESS_TOKEN is None:
        ACCESS_TOKEN, SESSION = get_access_token_via_refresh_token(REFRESH_TOKEN)
        logging.info(" Token initialized ONCE for all final() calls.")
    else:
        logging.info(" Token already initialized. Reusing existing session.")
        
        

share_links = [
    
    "https://magshield.sharepoint.com/:f:/s/Magshield/Eggs91M7-Y1Hqf_OGIpomVcBmsFhqwPKloOVrdk0RgveMg?e=RhN1Sq"
]

def upload_if_file_exists(file_path, label):
    """
    Uploads a file to OneDrive if it exists.
    Ensures file_path is valid and the file actually exists before uploading.
    """
    if file_path and os.path.exists(file_path):
        try:
            logging.info(f"Uploading {label}...")
            main_drive(share_links,ACCESS_TOKEN, upload_file=file_path)
            logging.info(f"{label} uploaded successfully.")
            os.remove(file_path)
        except Exception as e:
            logging.error(f"Failed to upload {label}: {e}", exc_info=True)
    else:
        logging.warning(f"{label} file not found or not created. Skipping upload.")

def final():
    quote_file = main_quote()
    upload_if_file_exists(quote_file, "Quote")
     

    organisation_file = main_organisation()
    upload_if_file_exists(organisation_file, "Organisation")
    

def final2():
     

    opportunity_file = main_opportunity()
    upload_if_file_exists(opportunity_file, "Opportunity")

def final3():
        
    euipmentfile = main_equipment_export()
    upload_if_file_exists(euipmentfile, "Equipment")
     
    
    invoice_file = main_invoice_export()
    upload_if_file_exists(invoice_file, "Invoice")
    
    
    users_file = main_users()
    upload_if_file_exists(users_file, "Users")
    
   
    
    
def final4():    
    task_file = main_task()
    upload_if_file_exists(task_file, "Task")

def final5():
    opportunity_stage = main_opp_stage(ACCESS_TOKEN, SESSION)        
    upload_if_file_exists(opportunity_stage, "Opportunity Stage")