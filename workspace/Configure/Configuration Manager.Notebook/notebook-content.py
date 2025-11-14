# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d92fe0c2-ddc2-4eb3-88f2-9601d96a182b",
# META       "default_lakehouse_name": "ReferenceDataLH",
# META       "default_lakehouse_workspace_id": "670345c3-e916-40a8-b57d-67e6c901cd2a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d92fe0c2-ddc2-4eb3-88f2-9601d96a182b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # ⚙️ Configuration Manager
# 
# ## Overview
# This notebook provides an interactive interface for managing solution configuration settings.
# 
# ## Features
# - **Dynamic Form Generation**: Input fields are automatically created based on the configuration array
# - **Persistent Storage**: Settings are saved to a Delta table for subsequent re-use
# - **Pre-filled Values**: Existing settings are automatically loaded when the form is displayed
# - **Validation**: Ensures all required fields are completed before saving
# - **Last Modified Tracking**: Automatically tracks when each setting was last updated
# 
# ## How to Use
# 1. Required configuration settings are pre-defined in the `config` variable
# 2. Run all cells and scroll down in the notebook to display the configuration form
# 3. Enter or update values in the input fields
# 4. Click **Submit** to save the configuration


# CELL ********************

# Import required libraries
import json
from datetime import datetime
from pathlib import Path

import ipywidgets as widgets
from IPython.display import display, clear_output
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import pyarrow as pa

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Configuration: Define your input fields here
config = [
    {
        "key": "AMI-Telemetry-EH-ConnectionString",
        "description": "Event hub connection string for AMI Telemetry.",
        "instructions": "Retrieve event hub connection string from the AMI_Eventstream source called AMISource. Navigate to the SAS Key Authentication section and copy the 'Connection string-primary key' value. The connection string will be in the following format: Endpoint=sb://yourendpoint.servicebus.windows.net/;SharedAccessKeyName=key_YourKeyName;SharedAccessKey=YourKeyValue=;EntityPath=YourEventHubName",
        "default_value": "Endpoint=sb://yourendpoint.servicebus.windows.net/;SharedAccessKeyName=key_YourKeyName;SharedAccessKey=YourKeyValue=;EntityPath=YourEventHubName"
    },
    {
        "key": "Vehicle-Telemetry-EH-ConnectionString",
        "description": "Event hub connection string for Vehicle Telemetry.",
        "instructions": "Retrieve event hub connection string from the Vehicle_Eventstream source called VehicleTelemetrySource. Navigate to the SAS Key Authentication section and copy the 'Connection string-primary key' value. The connection string will be in the following format: Endpoint=sb://yourendpoint.servicebus.windows.net/;SharedAccessKeyName=key_YourKeyName;SharedAccessKey=YourKeyValue=;EntityPath=YourEventHubName",
        "default_value": "Endpoint=sb://yourendpoint.servicebus.windows.net/;SharedAccessKeyName=key_YourKeyName;SharedAccessKey=YourKeyValue=;EntityPath=YourEventHubName"
    },
    {
        "key": "Weather-EH-ConnectionString",
        "description": "Event hub connection string for Weather.",
        "instructions": "Retrieve event hub connection string from the Weather_EventStream source called WeatherSource. Navigate to the SAS Key Authentication section and copy the 'Connection string-primary key' value. The connection string will be in the following format: Endpoint=sb://yourendpoint.servicebus.windows.net/;SharedAccessKeyName=key_YourKeyName;SharedAccessKey=YourKeyValue=;EntityPath=YourEventHubName",
        "default_value": "Endpoint=sb://yourendpoint.servicebus.windows.net/;SharedAccessKeyName=key_YourKeyName;SharedAccessKey=YourKeyValue=;EntityPath=YourEventHubName"
    }
]

# Define Delta table path
settings_table_path = "/lakehouse/default/Tables/settings8"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def ensure_settings_table_exists():
    """Create the settings Delta table if it doesn't exist."""
    table_path = Path(settings_table_path)
    
    if not table_path.exists():
        # Create empty DataFrame with schema
        empty_df = pd.DataFrame({
            "key": pd.Series(dtype=str),
            "value": pd.Series(dtype=str),
            "last_modified": pd.Series(dtype='datetime64[us]')
        })
        timestamp = datetime.now()
        df = pd.DataFrame({"key": ["version"], "value": ["1"], "last_modified":[timestamp]})
        write_deltalake(settings_table_path, df, mode="overwrite", schema_mode='merge', engine='rust', storage_options={"allow_unsafe_rename": "true"})
        print(f"✅ Created settings table at {settings_table_path}")
    else:
        print(f"✅ Settings table found at {settings_table_path}")


def load_existing_settings():
    """Load existing settings from the Delta table."""
    try:
        dt = DeltaTable(settings_table_path)
        df = dt.to_pandas()
        settings_dict = dict(zip(df['key'], df['value']))
        print(f"📥 Loaded {len(settings_dict)} existing settings")
        return settings_dict
    except Exception as e:
        print(f"⚠️ Could not load existing settings: {e}")
        return {}


def save_settings_to_delta(settings_data):
    """Save or update settings in the Delta table."""
    timestamp = datetime.now()
    
    # Prepare data for upsert
    data = {
        "key": list(settings_data.keys()),
        "value": list(settings_data.values()),
        "last_modified": [timestamp] * len(settings_data)
    }
    new_data = pd.DataFrame(data)
    # Perform upsert (merge) using delta-rs
    dt = DeltaTable(settings_table_path,storage_options={"allow_unsafe_rename": "true"}) 
    (dt.merge(
        source=new_data,
        predicate="target.key = source.key",
        source_alias="source",
        target_alias="target",)
    .when_matched_update(updates={"value": "source.value", "last_modified":"source.last_modified"})
    .when_not_matched_insert(
        updates={
            "key": "source.key",
            "value": "source.value", 
            "last_modified":"source.last_modified"
        })
    .execute()
    )
    print(f"✅ {datetime.now():%Y-%m-%d %H:%M:%S%z}: Saved {len(settings_data)} settings to Delta table")


# Ensure table exists
ensure_settings_table_exists()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 📝 Configuration Form
# Run the cell below to display the interactive configuration form. Existing values will be automatically loaded if available.

# CELL ********************

# Load existing settings
existing_settings = load_existing_settings()

# Create input widgets dynamically based on config
input_widgets = {}
widget_list = []

# Title
title = widgets.HTML(value="<h3>⚙️ Configuration Settings</h3>")
widget_list.append(title)

# Create a widget for each config item
for item in config:
    key = item["key"]
    description = item["description"]
    instructions = item["instructions"]
    default_value = item["default_value"]
    
    # Use existing value if available, otherwise use default
    initial_value = existing_settings.get(key, default_value)
    
    # Create label with instructions
    label = widgets.HTML(
        value=f"<b>{description}</b><br/><small style='color: #666;'>{instructions}</small>"
    )
    
    # Create text input widget
    text_widget = widgets.Text(
        value=initial_value,
        placeholder=f"Enter {description.lower()}...",
        layout=widgets.Layout(width='600px'),
        style={'description_width': '0px'}
    )
    
    input_widgets[key] = text_widget
    
    widget_list.append(label)
    widget_list.append(text_widget)
    widget_list.append(widgets.HTML(value="<br/>"))

# Create output area for messages
output = widgets.Output()

# Create submit button
submit_button = widgets.Button(
    description='💾 Submit',
    button_style='success',
    tooltip='Save configuration settings',
    icon='check',
    layout=widgets.Layout(width='200px', height='40px')
)

def on_submit_click(b):
    """Handle submit button click."""
    with output:
        #clear_output(wait=True)
        
        # Validate: check if all fields have values
        missing_fields = []
        settings_to_save = {}
        
        for item in config:
            key = item["key"]
            description = item["description"]
            value = input_widgets[key].value.strip()
            
            if not value:
                missing_fields.append(description)
            else:
                settings_to_save[key] = value
        timestamp = datetime.now()

        # If validation fails, show error
        if missing_fields:
            print(f"❌ {datetime.now():%Y-%m-%d %H:%M:%S%z}: Validation Error: The following fields are required:")
            for field in missing_fields:
                print(f"   • {field}")
            print("="*50) 
            return
        
        # Save to Delta table
        try:
            save_settings_to_delta(settings_to_save)
            print(f"\n✅ {datetime.now():%Y-%m-%d %H:%M:%S%z}: Configuration saved successfully!")
            print(f"\n📊 Saved settings:")
            for key, value in settings_to_save.items():
                # Mask sensitive values (connection strings, keys, etc.)
                if any(x in key.lower() for x in ['connection', 'key', 'secret', 'password']):
                    display_value = value[:20] + '...' if len(value) > 20 else value
                else:
                    display_value = value
                print(f"   • {key}: {display_value}")
        except Exception as e:
            print(f"❌ {datetime.now():%Y-%m-%d %H:%M:%S%z}: Error saving settings: {e}")

# Attach click handler
submit_button.on_click(on_submit_click)

# Add submit button and output to widget list
widget_list.append(submit_button)
widget_list.append(output)

# Display the form
form = widgets.VBox(widget_list, layout=widgets.Layout(padding='20px'))
display(form)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 📋 View Current Settings
# Run the cell below to view all current settings stored in the Delta table.

# CELL ********************

# Display current settings from Delta table
dt = DeltaTable(settings_table_path)
df = dt.to_pandas().sort_values("key")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
