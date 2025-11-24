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
# This notebook per
# - Sets data source credentials for semantic models to use the current user's OAuth token with SSO enabled
# - Triggers refresh of semantic models to ensure data is up-to-date after deployment

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