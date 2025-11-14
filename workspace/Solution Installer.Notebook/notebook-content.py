# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🚀 Fabric Solution Accelerator Deployment Notebook
# 
# This notebook orchestrates the end-to-end deployment of **Advanced Metering Infrastructure (AMI)** solution assets into the current Microsoft Fabric workspace using the `fabric-cicd` library.
# 
# ## This notebook performs the following tasks:
# 1. **📦 Package Installation**: Install required libraries and dependencies
# 1. **⚙️ Parameter Configuration and Library Import:** Configure parameters and import required libraries
# 1. **📥 Source Code Download**: Download and extracts solution content from GitHub repository
# 1. **🚀 Fabric Item Deployment**: Deploy Fabric items and map them to each other to preserve dependencies
# 1. **✅ Post-Deployment Tasks**: Complete post-deployment configuration tasks.

# MARKDOWN ********************

# 
# ## 📦 Package Installation

# CELL ********************

%pip install fabric-cicd --quiet
%pip install --upgrade azure-core azure-identity --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

print("⚠️ Restarting Python kernel for installed packages to take effect")
notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## ⚙️ Parameter Configuration and Library Import
# 
# Update these values to customize the deployment for your environment:

# CELL ********************

# Define user-configurable parameters
DEFAULT_API_ROOT_URL = "https://api.fabric.microsoft.com" #Default is https://api.fabric.microsoft.com, but may vary depending on your environment
DEBUG = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# The following settings should not be modified by the user
# GitHub Repository Configuration
repo_owner = "slavatrofimov"
repo_name = "amisandbox"
branch = "main"
folder_to_extract = "workspace"

# Deployment Configuration
deployment_environment = "DEV"  # Options: DEV, TEST, PROD

# File System Paths
path_prefix = '.lakehouse/default/Files'

extract_to_directory = path_prefix + "/src"
repository_directory = extract_to_directory + "/" + folder_to_extract

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

### Import libraries
import subprocess
import os
import json
from zipfile import ZipFile 
import shutil
import re
import requests
import zipfile
from io import BytesIO
import yaml
import sempy.fabric as fabric
import base64
from pathlib import Path
from typing import Optional, Any
from datetime import datetime, timezone
from azure.core.credentials import TokenCredential, AccessToken
import fabric_cicd.constants
from fabric_cicd import FabricWorkspace, publish_all_items

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 📥 Source Code Download
# 
# Download and extract solution source files and configuration from the GitHub repository:

# CELL ********************

# Download and extract GitRepository to a folder

def download_and_extract_folder(repo_owner, repo_name, extract_to, branch="main", 
                               folder_to_extract="workspace", remove_folder_prefix=""):
    """
    Download a GitHub repository and extract a specific folder directly to disk
    without saving the zip file.
    
    Args:
        repo_owner: GitHub repository owner
        repo_name: GitHub repository name
        extract_to: Local directory to extract files to
        branch: Git branch to download (default: "main")
        folder_to_extract: Folder path within the repo to extract
        remove_folder_prefix: Prefix to remove from extracted file paths
    """
    try:
        # Construct the URL for the GitHub API to download the repository as a zip file
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/zipball/{branch}"
        
        # Make a request to the GitHub API
        response = requests.get(url)
        response.raise_for_status()

        # Delete target directory if exists
        if os.path.exists(extract_to) and os.path.isdir(extract_to):
            shutil.rmtree(extract_to)
            print(f'Deleted existing directory: {extract_to}')
        
        # Ensure the extraction directory exists
        os.makedirs(extract_to, exist_ok=True)
        
        # Process the zip file directly from memory
        with zipfile.ZipFile(BytesIO(response.content)) as zipf:
            for file_info in zipf.infolist():
                # Check if the file is in the folder we want to extract
                normalized_path = re.sub(r'^.*?/', '/', file_info.filename)
                
                if normalized_path.startswith(f"/{folder_to_extract}"):
                    # Calculate the output path
                    parts = file_info.filename.split('/')
                    relative_path = '/'.join(parts[1:])  # Remove repo root folder
                    
                    # Remove the specified prefix if provided
                    if remove_folder_prefix:
                        relative_path = relative_path.replace(remove_folder_prefix, "", 1)
                    
                    output_path = os.path.join(extract_to, relative_path)
                    
                    # Skip if it's a directory entry
                    if file_info.filename.endswith('/'):
                        os.makedirs(output_path, exist_ok=True)
                        continue
                    
                    # Ensure the directory for the file exists
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    
                    # Extract and write the file
                    with zipf.open(file_info) as source_file:
                        with open(output_path, 'wb') as target_file:
                            target_file.write(source_file.read())
                            
        print(f"Successfully extracted {folder_to_extract} from {repo_owner}/{repo_name} to {extract_to}")
        
    except Exception as e:
        error_msg = f"A {type(e).__name__} error occurred. This error may be intermittent. Consider stopping the current notebook session and re-running the notebook again."
        print(error_msg)
        #  Re-raise the exception 
        raise

# Execute repo download and extraction using configured parameters
print(f"📥 Downloading {repo_name} from {repo_owner}/{repo_name}:{branch}")
print(f"📁 Extracting '{folder_to_extract}' folder to '{extract_to_directory}'")

download_and_extract_folder(
    repo_owner=repo_owner,
    repo_name=repo_name,
    extract_to=extract_to_directory,
    branch=branch,
    folder_to_extract=folder_to_extract,
    remove_folder_prefix=""
)

print("✅ Source code download and extraction completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 🚀 Fabric Item Deployment
# 
# Deploy solution assets to the current Fabric workspace:

# CELL ********************

# Custom Token Credential class for authentication in a Fabric Notebook
# This class enables the fabric-cicd library to be run in a Fabric notebook

class FabricNotebookTokenCredential(TokenCredential):
    """Token credential for Fabric Notebooks using notebookutils authentication."""
    
    def get_token(self, *scopes: str, claims: Optional[str] = None, tenant_id: Optional[str] = None,
                  enable_cae: bool = False, **kwargs: Any) -> AccessToken:
        """Get access token from Fabric notebook environment."""
        access_token = notebookutils.credentials.getToken("pbi")       
        expiration = self._extract_jwt_expiration(access_token)
        return AccessToken(token=access_token, expires_on=expiration)
    
    def _extract_jwt_expiration(self, token: str) -> int:
        """Extract expiration timestamp from JWT token."""
        try:
            # Split JWT and get payload (middle part)
            payload_b64 = token.split(".")[1]           
            # Add padding if needed for base64 decoding
            payload_b64 += "=" * (-len(payload_b64) % 4)
            # Decode and parse payload
            payload_bytes = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
            payload = json.loads(payload_bytes.decode("utf-8"))
            # Extract expiration claim
            exp = payload.get("exp")
            if exp is None:
                raise ValueError("JWT missing expiration claim")
            return exp
        except (IndexError, json.JSONDecodeError, ValueError) as e:
            raise ValueError(f"Invalid JWT token format: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Configure constants for fabric-cicd library
fabric_cicd.constants.DEFAULT_API_ROOT_URL = DEFAULT_API_ROOT_URL

# Get current workspace information
client = fabric.FabricRestClient()
workspace_id = fabric.get_workspace_id()
print(f"Target workspace ID: {workspace_id}")

# Enable debugging for more verbose execution logging
if DEBUG:
    from fabric_cicd import change_log_level
    change_log_level("DEBUG")

# Function to execute deployment of all specified item types
def deploy_artifacts(target_workspace):
    print("🚀 Starting deployment of Fabric items...")
    print(f"📋 Item types in scope: {', '.join(target_workspace.item_type_in_scope)}")
    publish_all_items(target_workspace)
    print("✅ Deployment completed successfully!")

# Initialize the FabricWorkspace object with configured parameters
target_workspace = FabricWorkspace(
    workspace_id=workspace_id,
    environment=deployment_environment,
    repository_directory=repository_directory,
    token_credential=FabricNotebookTokenCredential()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Start by deploying data stores
item_types_in_scope = [
    "Eventhouse", 
    "KQLDatabase", 
    "Lakehouse"
]

target_workspace.item_type_in_scope=item_types_in_scope

# Execute deployment of all specified item types
deploy_artifacts(target_workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Deploy remaining items
item_types_in_scope = [
    "Eventstream", 
    "Notebook", 
    "KQLDashboard", 
    "SemanticModel", 
    "Report", 
    "Reflex", 
    "DataAgent"
]

target_workspace.item_type_in_scope=item_types_in_scope

# Execute deployment of all specified item types
deploy_artifacts(target_workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## ✅ Post-Deployment Tasks

# MARKDOWN ********************

# ### 🔧 Automated Post-Deployment Tasks

# CELL ********************

# Move the KQL Database to the same subfolder as the Eventhouse
# To compensate for the incorrect placement of the item in the initial deployment

# Get current workspace information
client = fabric.FabricRestClient()
workspace_id = fabric.get_workspace_id()
eventhouse_id = fabric.resolve_item_id('PowerUtilitiesEH', 'Eventhouse')

# Step 1: move eventhose to the root folder
url = f'v1/workspaces/{workspace_id}/items/{kql_db_id}/move'

# Define payload for the API call and execute the call
payload = {} # Move to root folder
client.post(url, json = payload)

# Step 2: move eventhose (with the child KQL database) to the desired destination folder
# Define a function to get the id of the destination folder
def get_folder_id_by_name(folder_name: str, workspace_id: str) -> str | None:
    """Get folder ID by display name, returns None if not found."""
    try:

        url = f'v1/workspaces/{workspace_id}/folders'
        folders = client.get(url)
        for folder in folders.json()["value"]:
            if folder["displayName"] == 'Store and Query':
                return(folder["id"])
        
    except Exception as e:
        print(f"Error: {e}")
        return None

# Get target folder
target_folder_id = get_folder_id_by_name('Store and Query', workspace_id)

# Define payload for the API call and execute the call
payload = {
  "targetFolderId": f"{target_folder_id}"
}

client.post(url, json = payload)
print("✅ Eventhouse moved successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Attach the notebook to the ReferenceDataLH Lakehouse and restart the Python kernel. Note that all imports and variable definitions will be lost.

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC      "defaultLakehouse": {  
# MAGIC         "name": "ReferenceDataLH"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Import sample data from the Git repository and 

import requests

repo_owner = "slavatrofimov"
repo_name = "amisandbox"
branch = "main"
file_path = "data/vehicle_route_points.json"
target_directory = "/lakehouse/default/Files/data"

def download_file(repo_owner, repo_name, branch, file_path, target_directory):
    """
    Download and save a file from a GitHub repository.
    
    Args:
        repo_owner: GitHub repository owner
        repo_name: GitHub repository name
        branch: Git branch to download (default: "main")
        file_path: File path within the repo to download
        target_directory: Directory where to save the file
    """

    # Download sample data file
    file_url = f"https://raw.githubusercontent.com/{repo_owner}/{repo_name}/refs/heads/{branch}/{file_path}"
    file_name = file_url.split("/")[-1]  # Extract filename from URL

    try:
        # Create target directory if it doesn't exist
        os.makedirs(target_directory, exist_ok=True)
        
        # Download the file
        print(f"📥 Downloading file from {file_url}")
        response = requests.get(file_url)
        response.raise_for_status()
        
        # Save to target directory
        target_path = os.path.join(target_directory, file_name)
        with open(target_path, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ File saved successfully to {target_path}")
        
    except requests.RequestException as e:
        print(f"❌ Error downloading file: {e}")
        raise
    except Exception as e:
        print(f"❌ Error saving file: {e}")
        raise

download_file(repo_owner, repo_name, branch, file_path, target_directory)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### ⚙️ Manual Post-Deployment Steps
# Complete the following tasks to finish the installation
# 1. 🔄 Generate reference data by running the **AMI Reference Data Simulation** notebook (in the Simulation folder)
