

```
Follow these steps to set up and run the project locally.

0. Clone the Repository

Start by cloning the repository to your local machine:

git clone https://github.com/hussain-magshield/magshield_data_pipeline.git
cd magshield_data_pipeline


1. Create a Virtual Environment

python -m venv env


2. Activate the Environment

On Mac/Linux:
source env/bin/activate

On Windows:
env\Scripts\activate


3. Install Dependencies

pip install -r requirements.txt


4. Create Environment Configuration File

Create a new file named env.yaml in the project root and add the following:

CLIENT_ID: ""
TENANT_ID: ""
REFRESH_TOKEN: ""
INSIGHTLY_API_KEY: ""


5. Deploy Code to Azure Cloud

Push the code to GitHub. It will automatically deploy to Azure:

git add .
git commit -m "new project files"
git push origin main


6. Troubleshooting Authentication Errors (Microsoft / Azure)

If you face an authentication error:

• Go to Azure Portal  
• Navigate to:  
  App Registration → Microsoft 365 Backup  
  → Manage  
  → Certificates & Secrets  
  → Click "New Client Secret"  

• Copy the newly generated secret VALUE immediately.

• Go to:  
  Function App → insightly data pipeline  
  → Settings  
  → Environment Variables  

• Replace the old CLIENT_SECRET value with the new secret value.  
• Save and restart the Function App.


7. Insightly API Key Update

If Insightly API errors occur:

• Log in to the Insightly Portal  
• Generate a new API Key  
• Update the INSIGHTLY_API_KEY value in:
 
  - Azure Function App → Environment Variables (for production)

  

```
