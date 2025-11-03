import time
import os
import logging
from modules.one_drive import main_drive
from modules.quote import main_quote
from modules.task import main_task
from modules.organisation import main_organisation
from modules.opportunity import main_opportunity
from modules.equiment import main_equipment_export
from modules.invoice import main_invoice_export

share_links = [
    # "https://magshield.sharepoint.com/sites/Magshield/Shared%20Documents/Forms/AllItems.aspx?FolderCTID=0x012000E50C8FA1B0C81A4C9C26E6E96200FCDC&id=%2Fsites%2FMagshield%2FShared%20Documents%2FMagShield%20General%20Drive%2F10%2E%20Systems%2F10%2E5%20PowerBI%2FData%20Feeding%20Reports",
    "https://magshield-my.sharepoint.com/:f:/p/hussainm/Emqg670lFK5OuYdv5YBNTawBBou1_WCXMnJo3ouz73KZ0g"
]

def upload_if_file_exists(file_path, label):
    """
    Uploads a file to OneDrive if it exists.
    Ensures file_path is valid and the file actually exists before uploading.
    """
    if file_path and os.path.exists(file_path):
        try:
            logging.info(f"Uploading {label}...")
            main_drive(share_links, upload_file=file_path)
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
    task_file = main_task()
    upload_if_file_exists(task_file, "Task")
   

    opportunity_file = main_opportunity()
    upload_if_file_exists(opportunity_file, "Opportunity")

def final3():
        
    euipmentfile = main_equipment_export()
    upload_if_file_exists(euipmentfile, "Equipment")
     
    
    invoice_file = main_invoice_export()
    upload_if_file_exists(invoice_file, "Invoice")
