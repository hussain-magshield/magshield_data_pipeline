

```
Follow these steps to set up and run the project locally.


0. Clone the Repository

 Start by cloning the repository to your local machine:


 git clone https://github.com/hussain-magshield/magshield_data_pipeline.git
 cd magshield_data_pipeline

1. Create a Virtual Environment
    
 python -m venv env

2. Activate the Environment
 
 source env/bin/activate

3. Install Dependencies

 pip install -r requirements.txt

4. Create Environment Configuration File

 Create a new file named env.yaml in the project root and add the following:

 CLIENT_ID: ""
 TENANT_ID: ""
 REFRESH_TOKEN: ""
 INSIGHTLY_API_KEY: ""
 
 

 

5. To Deploy code to Azure Cloud , Push Code to GitHub and it will automatically deploy to cloud

    git add .
    git commit -m "new project files"
    git push origin main

 

```
