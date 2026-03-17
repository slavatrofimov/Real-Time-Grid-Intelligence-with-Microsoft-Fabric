# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# # ⚙️ Post-Deployment Configuration
# 
# - Deploys a Fabric Map item for geospatial visualization
# - Sets data source credentials for semantic models to use the current user's OAuth token with SSO enabled
# - Triggers refresh of semantic models to ensure data is up-to-date after deployment
# - Trigger execution of a notebook to populate reference data for the solution
# - Creates accelerated shortcuts in the KQL database for improved query performance
# - Executes a KQL command to load data into the MeterContextualization table in order to populate it with data for use in the solution

# CELL ********************

%pip install fabric-launcher --quiet
import notebookutils
notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#Import required libraries
import sempy.fabric as fabric
from fabric_launcher import create_or_update_fabric_item, get_folder_id_by_name, move_item_to_folder, scan_logical_ids
from fabric_launcher import FabricLauncher

# Download source code from GitHub
launcher = FabricLauncher(notebookutils,
    api_root_url = "https://api.fabric.microsoft.com" #Default is https://api.fabric.microsoft.com, but may vary depending on your environment
    )

extract_to = 'src'
launcher.download_repository(
    repo_owner="slavatrofimov",
    repo_name="Real-Time-Grid-Intelligence-with-Microsoft-Fabric",
    extract_to=extract_to,
    branch="continuous-simulation",
)

# Initialize Fabric client and workspace
client = fabric.FabricRestClient()
workspace_id = fabric.get_workspace_id()
repository_directory = f"./{extract_to}/workspace/"

def is_notebook_running(notebook_name, workspace_id=workspace_id, client=client):
    """Check if a notebook has an InProgress job instance using the Fabric Job Scheduler API."""
    notebooks = fabric.list_items(type="Notebook")
    match = notebooks[notebooks['Display Name'] == notebook_name]
    if match.empty:
        return False
    item_id = match.iloc[0]['Id']
    url = f"v1/workspaces/{workspace_id}/items/{item_id}/jobs/instances"
    response = client.get(url)
    if response.status_code == 200:
        for instance in response.json().get('value', []):
            if instance.get('status') in ('InProgress', 'NotStarted'):
                return True
    return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

## Deploy the Service Area Map item
def deploy_map(item_name="ServiceAreaMap", item_type="Map", item_relative_path="grid-intelligence/VisualizeAndChat/ServiceAreaMap.Map", 
               folder_name="VisualizeAndChat", endpoint="maps", repository_directory = repository_directory,
               description="Service Area Map for Real-Time Grid Intelligence solution",
               client = client, workspace_id = workspace_id):
    """Use fabric_launcher utilities to deploy an additional item: Map"""

    # Check if the map item already exists in the workspace
    existing_items = fabric.list_items(type=item_type)
    if not existing_items.empty and item_name in existing_items['Display Name'].values:
        print(f"ℹ️ '{item_name}' already exists in the workspace. Skipping deployment.")
        return

    # Step 1: Scan logical IDs
    print("1. Scanning logical IDs in repository...")
    logical_id_map = scan_logical_ids(
        repository_directory=repository_directory, workspace_id=workspace_id, client=client
    )

    # Step 2: Create/update a custom item with logical ID replacement
    print("2. Creating/updating custom Fabric item...")
    item_id = create_or_update_fabric_item(
        item_name=item_name,
        item_type=item_type,
        item_relative_path=item_relative_path,
        repository_directory=repository_directory,
        workspace_id=workspace_id,
        client=client,
        endpoint=endpoint,
        logical_id_map=logical_id_map,
        description=description,
    )
    print(f"   Item ID: {item_id}")

    # Step 3: Move item to appropriate folder
    print("3. Moving item to target folder...")
    success = move_item_to_folder(
        item_name=item_name,
        item_type=item_type,
        folder_name=folder_name,
        workspace_id=workspace_id,
        client=client
    )

    if success:
        print("✅ Map created successfully")

# Perform the deployment of the Service Area Map
deploy_map()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import sempy.fabric as fabric
import requests

### Update data source credentials and refresh semantic models
def update_semantic_model_datasource_credentials(semantic_model_id):
    """
    Update datasource credentials for the selected semantic model using the OAuth access token of the current user and enable SSO
    """    
    try:

        workspace_id = fabric.get_workspace_id()

        client = fabric.PowerBIRestClient()
        datasources_url = f'v1.0/myorg/groups/{workspace_id}/datasets/{semantic_model_id}/datasources'
        datasources_response = client.get(datasources_url)

        if datasources_response.status_code == 200:
            datasources = datasources_response.json().get('value', [])
            print(f"📊 Found {len(datasources)} datasource(s) in semantic model")
            
            # Update credentials for each datasource to use workspace identity
            for datasource in datasources:
                datasource_id = datasource.get('datasourceId')
                gateway_id = datasource.get('gatewayId')
                datasource_type = datasource.get('datasourceType', 'Unknown')
                
                print(f"🔧 Updating credentials for datasource: {datasource_type} (ID: {datasource_id})")
                
                access_token = notebookutils.credentials.getToken('kusto')

                # Prepare credentials payload for workspace identity authentication
                credentials_payload = {
                    "credentialDetails": {
                        "credentialType": "OAuth2",
                        "credentials": "{\"credentialData\":[{\"name\":\"accessToken\", \"value\":\"" + access_token + "\"}]}",
                        "encryptedConnection": "Encrypted",
                        "encryptionAlgorithm": "None",
                        "privacyLevel": "None",
                        "useEndUserOAuth2Credentials": "True",
                        "useCallerAADIdentity": "False"
                    }
                }
                
                # Update datasource credentials
                update_creds_url = f'v1.0/myorg/gateways/{gateway_id}/datasources/{datasource_id}'
                creds_response = client.patch(update_creds_url, json=credentials_payload)
                
                if creds_response.status_code in [200, 201]:
                    print(f"✅ Credentials updated successfully for datasource {datasource_type}")
                else:
                    print(f"⚠️ Warning: Could not update credentials for datasource {datasource_type}: {creds_response.status_code} - {creds_response.text}")
                    
        else:
            print(f"⚠️ Warning: Could not retrieve datasources: {datasources_response.status_code}")

    except Exception as e:
        print(f"⚠️ Warning: Error updating semantic model credentials: {e}")
        raise

def refresh_semantic_model(semantic_model_id):
    """
    Trigger refresh of a semantic model
    """    
    workspace_id = fabric.get_workspace_id()

    try:
        if semantic_model_id:
            # Trigger semantic model refresh
            client = fabric.PowerBIRestClient()
            refresh_url = f'v1.0/myorg/groups/{workspace_id}/datasets/{semantic_model_id}/refreshes'
            refresh_payload = {
                "retryCount": 2
                }
            
            refresh_response = client.post(refresh_url, json=refresh_payload)
            
            if refresh_response.status_code in [200, 201, 202]:
                refresh_data = refresh_response.text
                print(refresh_data)
                print("ℹ️ Triggered semantic model refresh. Refresh will continue in the background.")
            else:
                print(f"⚠️ Warning: Could not trigger semantic model refresh: {refresh_response.status_code} - {refresh_response.text}")
        else:
            print("⚠️ Warning: Cannot trigger refresh - semantic model not found")
            
    except Exception as e:
        print(f"⚠️ Warning: Error triggering semantic model refresh: {e}")
        raise


# Update datasource credentials and refresh all semantic models in the current workspace

# Get all semantic models in the workspace
semantic_models = fabric.list_items(type="SemanticModel")

if semantic_models.empty:
    print("No semantic models found in the workspace.")
else:
    print(f"Total semantic models found: {len(semantic_models)}")
    for index, model in semantic_models.iterrows():
        print("="*60)
        print(f"Starting to process semantic model: {model['Display Name']} (ID: {model['Id']})")
        semantic_model_id = model['Id']
        update_semantic_model_datasource_credentials(semantic_model_id)
        refresh_semantic_model(semantic_model_id)
        print(f"Processed semantic model: {model['Display Name']} (ID: {model['Id']})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Load AMIReferenceDataSimulation notebook to populate reference data (skip if data already exists)
from fabric_launcher.post_deployment_utils import (
    get_kusto_query_uri,
    exec_kql_command
)

skip_reference_data = False
try:
    _query_uri = get_kusto_query_uri(fabric.resolve_workspace_id(), 'PowerUtilitiesEH', client)
    _result = exec_kql_command(_query_uri, 'PowerUtilitiesEH', '.show table MeterContextualization data statistics | summarize max(PresentRowCount)', notebookutils)
    _count = _result['Tables'][0]['Rows'][0][0]
    if _count > 0:
        print(f"ℹ️ MeterContextualization table already contains {_count} row(s). Skipping reference data generation.")
        skip_reference_data = True
except Exception as e:
    # Table likely does not exist yet — proceed with reference data generation
    print(f"ℹ️ MeterContextualization table not found or empty ({e}). Proceeding with reference data generation.")

if skip_reference_data:
    pass
elif is_notebook_running("AMIReferenceDataSimulation"):
    print("ℹ️ 'AMIReferenceDataSimulation' is already running. Skipping execution.")
else:
    result = launcher.run_notebook_synchronous(
        notebook_name="AMIReferenceDataSimulation",
        parameters={},
        timeout_seconds=3600
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Create accelerated shortcuts in the KQL Database
from fabric_launcher.post_deployment_utils import (
    get_kusto_query_uri,
    create_accelerated_shortcut_in_kql_db
)

# Configuration Parameters
target_workspace_id = fabric.resolve_workspace_id()
target_eventhouse_name = 'PowerUtilitiesEH'
target_kql_db_name = 'PowerUtilitiesEH'
source_workspace_id = target_workspace_id 
source_lakehouse_name = 'ReferenceDataLH'

# Create shortcuts for required tables
tables = ['feeders', 'meters', 'substations', 'transformers']

if skip_reference_data:
    print("ℹ️ Skipping accelerated shortcut creation since reference data generation was skipped.")
else:    
    for table in tables:
        source_path = f"Tables/{table}" 
        target_shortcut_name = table.capitalize()
        
        print(f"Creating accelerated shortcut for table: {table}")
        
        try:
            create_accelerated_shortcut_in_kql_db(
                notebookutils=notebookutils,
                target_workspace_id=target_workspace_id,
                target_eventhouse_name=target_eventhouse_name,
                target_kql_db_name=target_kql_db_name,
                target_shortcut_name=target_shortcut_name,
                source_workspace_id=source_workspace_id,
                source_lakehouse_name=source_lakehouse_name,
                source_path=source_path,
                client=client
            )
            print(f"✅ Successfully created accelerated shortcut for '{table}'")
            
        except Exception as e:
            print(f"❌ Failed to create shortcut for '{table}': {str(e)}")
            # Continue with next table instead of stopping
            continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Execute a KQL Query to load data into the MeterContextualization table
from fabric_launcher.post_deployment_utils import (
    get_kusto_query_uri,
    exec_kql_command
)

if skip_reference_data:
    print("ℹ️ Skipping data load into MeterContextualization table since reference data generation was skipped.")
else:
    kusto_query_uri = get_kusto_query_uri(target_workspace_id, target_eventhouse_name, client)
    kql_command = f""".set-or-replace MeterContextualization <| MeterContextualizationFunction()"""
    exec_kql_command(kusto_query_uri, target_kql_db_name, kql_command, notebookutils)
    print('✅ Loaded data into the MeterContextualiazation table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Initiate event simulation notebooks to populate telemetry and outage data, vehicle telemetry and weather data. 
# These notebooks will run asynchronously.
for nb_name in ["AMITelemetryAndOutageSimulation", "VehicleTelemetrySimulator", "StormSimulation"]:
    if is_notebook_running(nb_name):
        print(f"ℹ️ '{nb_name}' is already running. Skipping execution.")
    else:
        result = launcher.run_notebook(
            notebook_name=nb_name,
            parameters={},
            timeout_seconds=7200
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }



# MARKDOWN ********************

# ## Next Steps
# Your automated solution deploymnet is complete!
# 
# ⚠️ Please be sure to refresh your browser window to reflect all newly-deployed items!
# 
#  Review the README document for details on running simulations and exploring your solution.