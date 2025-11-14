# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ff897dd5-242e-4479-ad83-9eff7247de52",
# META       "default_lakehouse_name": "ReferenceDataLH",
# META       "default_lakehouse_workspace_id": "7a647ab0-2135-435a-9419-bf9f48a0af54",
# META       "known_lakehouses": [
# META         {
# META           "id": "ff897dd5-242e-4479-ad83-9eff7247de52"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # AMI Smart Meter Reference Data Simulation
# 
# Generates simulated reference data for an Advanced Metering Infrastructure (AMI) system, including substations, feeder lines, transformers, and smart meters with realistic network topology.
# 
# **Output**: Delta tables in Lakehouse containing reference data for ~4,000-6,000 meters organized by electrical grid hierarchy.


# MARKDOWN ********************

# ## Setup

# CELL ********************

# Install required packages
%pip install faker pandas numpy --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np

from faker import Faker
from faker.providers import automotive, internet, phone_number

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Faker with seed for reproducible results
fake = Faker()
fake.add_provider(automotive)
fake.add_provider(internet)
fake.add_provider(phone_number)

random.seed(42)
np.random.seed(42)
Faker.seed(42)

print("✅ Libraries imported successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Geographic Boundaries

# CELL ********************

# City center coordinates
CITY_CENTER = {
    'lat': 39.76838,
    'lon': -86.15804
}

# City boundaries (approximate rectangular area)
CITY_BOUNDS_NORTH = CITY_CENTER['lat'] + 0.20
CITY_BOUNDS_SOUTH = CITY_CENTER['lat'] - 0.20
CITY_BOUNDS_EAST = CITY_CENTER['lon'] + 0.15
CITY_BOUNDS_WEST = CITY_CENTER['lon'] - 0.15

CITY_BOUNDS = {
    'north': CITY_BOUNDS_NORTH,
    'south': CITY_BOUNDS_SOUTH,
    'east': CITY_BOUNDS_EAST,
    'west': CITY_BOUNDS_WEST
}

# City dimensions in degrees
CITY_WIDTH = CITY_BOUNDS['east'] - CITY_BOUNDS['west']  
CITY_HEIGHT = CITY_BOUNDS['north'] - CITY_BOUNDS['south']

print(f"City boundaries configured:")
print(f"  Width: {CITY_WIDTH:.4f}° | Height: {CITY_HEIGHT:.4f}°")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate Network Topology
# 
# Creates hierarchical grid structure: substations → feeders → transformers → meters

# CELL ********************

def generate_network_topology():
    """Generate realistic electrical grid topology with all network elements."""
    
    print("Generating network topology...")
    
    # ═══════════════════════════════════════════════════════════
    # Substations (12 total)
    # ═══════════════════════════════════════════════════════════
    substations = []
    substation_count = 12
    substation_names = [fake.city() + " Sub" for _ in range(substation_count)]
    
    for i, name in enumerate(substation_names):
        substation = {
            "substation_id": f"SUB_{str(i+1).zfill(3)}",
            "substation_name": name,
            "voltage_level": "12.47kV",
            "capacity_mva": random.randint(25, 100),
            "latitude": CITY_CENTER['lat'] + np.random.normal(-0.10, 0.10),
            "longitude": CITY_CENTER['lon'] + np.random.normal(-0.12, 0.12),
            "commissioned_date": fake.date_between(start_date='-20y', end_date='-5y').isoformat()
        }
        substations.append(substation)
    
    print(f"  → Created {len(substations)} substations")
    
    # ═══════════════════════════════════════════════════════════
    # Feeder Lines (5-10 per substation)
    # ═══════════════════════════════════════════════════════════
    feeders = []
    counter = 0
    
    for sub in substations:
        feeder_count = 5 + random.randint(0, 5)
        for i in range(feeder_count):
            # Assign a radial angle for this feeder (distribute evenly around substation)
            angle = (2 * np.pi * i) / feeder_count + np.random.uniform(-0.2, 0.2)
            
            feeder = {
                "feeder_id": f"FDR_{str(counter+1).zfill(3)}",
                "feeder_name": f"Feeder_{chr(65 + (i % 26))}",
                "substation_id": sub["substation_id"],
                "voltage_level": "12.47kV",
                "max_load_kw": random.randint(3000, 8000),
                "length_miles": random.uniform(2.5, 12.0),
                "conductor_type": random.choice(["ACSR", "Aluminum", "Copper"]),
                "protection_scheme": random.choice(["OCR", "Distance", "Differential"]),
                "scada_monitored": random.choice([True, False]),
                "latitude": sub["latitude"],
                "longitude": sub["longitude"],
                # Store the radial angle for transformer positioning
                "_radial_angle": angle
            }
            feeders.append(feeder)
            counter += 1
    
    print(f"  → Created {len(feeders)} feeder lines")
    
    # ═══════════════════════════════════════════════════════════
    # Transformers (25-100 per feeder)
    # ═══════════════════════════════════════════════════════════
    transformers = []
    counter = 0
    
    # Need to lookup substation for each feeder
    substation_lookup = {s["substation_id"]: s for s in substations}
    
    for feeder in feeders:
        transformer_count = 25 + random.randint(0, 75)
        sub = substation_lookup[feeder["substation_id"]]
        base_angle = feeder["_radial_angle"]
        
        for i in range(transformer_count):
            # Place transformers along a line radiating from the substation
            # Distance increases with each transformer (with some randomness)
            distance = (i + 1) * (feeder["length_miles"] / transformer_count) / 69.0  # Convert miles to degrees (approx)
            distance *= random.uniform(0.005, 0.25)  # Add distance variation
            
            # Add small angular jitter to the radial angle (keeps them roughly on the line)
            angle_jitter = np.random.normal(0, 0.05)  # Small jitter in radians
            actual_angle = base_angle + angle_jitter
            
            # Calculate position using polar coordinates relative to substation
            lat_offset = distance * np.sin(actual_angle)
            lon_offset = distance * np.cos(actual_angle)
            
            transformer = {
                "transformer_id": f"XFMR_{str(counter+1).zfill(4)}",
                "feeder_id": feeder["feeder_id"],
                "transformer_type": random.choice(["Pad-mounted", "Pole-mounted", "Underground", "Vault"]),
                "primary_voltage": 12470,
                "secondary_voltage": random.choice([120, 240, 480]),
                "kva_rating": random.choice([25, 50, 75, 100, 167, 250, 500]),
                "phase_config": random.choice(["Single", "Three"]),
                "install_date": fake.date_between(start_date='-15y', end_date='today').isoformat(),
                "manufacturer": random.choice(["ACE", "Schroeder", "Edison", "Special Electrics", "Hooper"]),
                "latitude": float(sub["latitude"] + lat_offset),
                "longitude": float(sub["longitude"] + lon_offset),
                "load_tap_changer": random.choice([True, False]),
                "temperature_monitoring": random.choice([True, False])
            }
            transformers.append(transformer)
            counter += 1
    
    print(f"  → Created {len(transformers)} transformers")
    
    # ═══════════════════════════════════════════════════════════
    # Smart Meters
    # ═══════════════════════════════════════════════════════════
    METER_MODELS = [
        {"manufacturer": "Daxtron", "model": "Centaur Z2SOR", "type": "residential", "max_amps": 200, "voltage": 240, "phases": 1},
        {"manufacturer": "Landex+Grid", "model": "K650", "type": "residential", "max_amps": 200, "voltage": 240, "phases": 1},
        {"manufacturer": "Sentus", "model": "ReCon A", "type": "residential", "max_amps": 320, "voltage": 240, "phases": 1},
        {"manufacturer": "Daxtron", "model": "Centaur Z1S", "type": "commercial", "max_amps": 400, "voltage": 480, "phases": 3},
        {"manufacturer": "Landex+Grid", "model": "E850", "type": "commercial", "max_amps": 400, "voltage": 480, "phases": 3},
        {"manufacturer": "GX", "model": "A-210+c", "type": "residential", "max_amps": 200, "voltage": 240, "phases": 1},
        {"manufacturer": "Schroeder Electric", "model": "Scion7550", "type": "industrial", "max_amps": 800, "voltage": 480, "phases": 3},
        {"manufacturer": "Honeyville", "model": "Falcon", "type": "residential", "max_amps": 200, "voltage": 240, "phases": 1}
    ]
    
    meters = []
    counter = 0
    
    for transformer in transformers:
        # Use exponential decay probability: most transformers have 1 meter, few have many
        # Generate a random value and map it to meter count using exponential distribution
        rand_val = random.random()
        if rand_val < 0.60:  # 60% have exactly 1 meter
            meter_count = 1
        elif rand_val < 0.85:  # 25% have 2-3 meters
            meter_count = random.randint(2, 3)
        elif rand_val < 0.95:  # 10% have 4-6 meters
            meter_count = random.randint(4, 6)
        elif rand_val < 0.99:  # 4% have 7-12 meters
            meter_count = random.randint(7, 12)
        else:  # 1% have 13-25 meters (large commercial/industrial)
            meter_count = random.randint(13, 25)
        
        for i in range(meter_count):
            # Select meter model based on realistic distribution
            meter_type_prob = random.random()
            if meter_type_prob < 0.75:  # 75% residential
                available_models = [m for m in METER_MODELS if m["type"] == "residential"]
            elif meter_type_prob < 0.95:  # 20% commercial
                available_models = [m for m in METER_MODELS if m["type"] == "commercial"]
            else:  # 5% industrial
                available_models = [m for m in METER_MODELS if m["type"] == "industrial"]
            
            model_spec = random.choice(available_models)
            
            # Installation date (60% in last 3 years, 40% older)
            if random.random() < 0.6:
                install_date = fake.date_between(start_date='-3y', end_date='today')
            else:
                install_date = fake.date_between(start_date='-10y', end_date='-3y')
            
            # Generate coordinates near transformer
            base_lat, base_lon = transformer["latitude"], transformer["longitude"]
            
            meter = {
                "meter_id": f"MTR{str(counter+1).zfill(6)}",
                "serial_number": fake.uuid4()[:8].upper(),
                "manufacturer": model_spec["manufacturer"],
                "model": model_spec["model"],
                "meter_type": model_spec["type"],
                "max_amps": model_spec["max_amps"],
                "voltage": model_spec["voltage"],
                "phases": model_spec["phases"],
                "install_date": install_date.isoformat(),
                "firmware_version": f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,99)}",
                "communication_type": random.choice(["RF Mesh", "Cellular", "PLC", "Fiber"]),
                "street_address": fake.street_address(),
                "city": fake.city(),
                "state": "IN",
                "zip_code": str(random.randint(46201, 46247)),
                "latitude": base_lat + np.random.normal(0, 0.008),
                "longitude": base_lon + np.random.normal(0, 0.008),
                "customer_id": f"CUST{str(random.randint(100000, 999999))}",
                "customer_type": model_spec["type"],
                "service_class": random.choice(["Residential", "Commercial", "Industrial"]) if model_spec["type"] != "residential" else "Residential",
                "accuracy_class": random.choice(["0.2%", "0.5%", "1.0%"]),
                "register_type": random.choice(["Digital", "LCD", "LED"]),
                "pulse_output": random.choice([True, False]),
                "tamper_detection": True,
                "power_quality_monitoring": random.choice([True, False]),
                "load_profile_interval": random.choice([5, 15, 30, 60]),
                "status": "active",
                "last_maintenance": fake.date_between(start_date=install_date, end_date='today').isoformat(),
                "transformer_id": transformer["transformer_id"]
            }
            meters.append(meter)
            counter += 1
            
            # Progress indicator every 500 meters
            if counter % 500 == 0:
                print(f"  → Generated {counter} meters...")
    
    print(f"  → Created {len(meters)} meters")
    
    # ═══════════════════════════════════════════════════════════
    # Create DataFrames
    # ═══════════════════════════════════════════════════════════
    
    substation_schema = StructType([
        StructField("substation_id", StringType(), False),
        StructField("substation_name", StringType(), False),
        StructField("voltage_level", StringType(), False),
        StructField("capacity_mva", IntegerType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("commissioned_date", StringType(), False)
    ])
    
    feeder_schema = StructType([
        StructField("feeder_id", StringType(), False),
        StructField("feeder_name", StringType(), False),
        StructField("substation_id", StringType(), False),
        StructField("voltage_level", StringType(), False),
        StructField("max_load_kw", IntegerType(), False),
        StructField("length_miles", DoubleType(), False),
        StructField("conductor_type", StringType(), False),
        StructField("protection_scheme", StringType(), False),
        StructField("scada_monitored", BooleanType(), False)
    ])
    
    transformer_schema = StructType([
        StructField("transformer_id", StringType(), False),
        StructField("feeder_id", StringType(), False),
        StructField("transformer_type", StringType(), False),
        StructField("primary_voltage", IntegerType(), False),
        StructField("secondary_voltage", IntegerType(), False),
        StructField("kva_rating", IntegerType(), False),
        StructField("phase_config", StringType(), False),
        StructField("install_date", StringType(), False),
        StructField("manufacturer", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("load_tap_changer", BooleanType(), False),
        StructField("temperature_monitoring", BooleanType(), False)
    ])

    meter_schema = StructType([
        StructField("meter_id", StringType(), False),
        StructField("serial_number", StringType(), False),
        StructField("manufacturer", StringType(), False),
        StructField("model", StringType(), False),
        StructField("meter_type", StringType(), False),
        StructField("max_amps", IntegerType(), False),
        StructField("voltage", IntegerType(), False),
        StructField("phases", IntegerType(), False),
        StructField("install_date", StringType(), False),
        StructField("firmware_version", StringType(), False),
        StructField("communication_type", StringType(), False),
        StructField("street_address", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("zip_code", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_type", StringType(), False),
        StructField("service_class", StringType(), False),
        StructField("accuracy_class", StringType(), False),
        StructField("register_type", StringType(), False),
        StructField("pulse_output", BooleanType(), False),
        StructField("tamper_detection", BooleanType(), False),
        StructField("power_quality_monitoring", BooleanType(), False),
        StructField("load_profile_interval", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("last_maintenance", StringType(), False),
        StructField("transformer_id", StringType(), False),
    ])
    
    substations_df = spark.createDataFrame(substations, schema=substation_schema)
    feeders_df = spark.createDataFrame(feeders, schema=feeder_schema)
    transformers_df = spark.createDataFrame(transformers, schema=transformer_schema)
    meters_df = spark.createDataFrame(meters, schema=meter_schema)
    
    print("\n✅ Network topology generation complete")
    
    return substations_df, feeders_df, transformers_df, meters_df

# Generate topology
substations_df, feeders_df, transformers_df, meters_df = generate_network_topology()

print(f"\n📊 Summary:")
print(f"  Substations:  {substations_df.count():,}")
print(f"  Feeders:      {feeders_df.count():,}")
print(f"  Transformers: {transformers_df.count():,}")
print(f"  Meters:       {meters_df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save to Lakehouse Tables

# CELL ********************

# Configuration: Set to True to overwrite existing tables (use with caution)
OverwriteExisting = True

# Get existing tables
existing_tables = [table.name for table in notebookutils.lakehouse.listTables()]

# Save substations
if 'substations' not in existing_tables or OverwriteExisting:
    substations_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("substations")
    print('✅ Saved substations table')
else:
    print('⏭️  Substations table already exists (skipped)')
  
# Save feeders
if 'feeders' not in existing_tables or OverwriteExisting:
    feeders_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("feeders")
    print('✅ Saved feeders table')
else:
    print('⏭️  Feeders table already exists (skipped)')

# Save transformers
if 'transformers' not in existing_tables or OverwriteExisting:
    transformers_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("transformers")
    print('✅ Saved transformers table')
else:
    print('⏭️  Transformers table already exists (skipped)')

# Save meters
if 'meters' not in existing_tables or OverwriteExisting:
    meters_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("meters")
    print('✅ Saved meters table')
else:
    print('⏭️  Meters table already exists (skipped)')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## View Sample Data

# CELL ********************

print("Sample Substations:")
display(substations_df.limit(5))

print("\nSample Feeders:")
display(feeders_df.limit(5))

print("\nSample Transformers:")
display(transformers_df.limit(5))

print("\nSample Meters:")
display(meters_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create KQL Database Shortcuts
# 
# Creates shortcuts and accelerated external tables in the KQL Database for query access.

# CELL ********************

import sempy.fabric as fabric
import requests

# Configuration
target_workspace_id = fabric.resolve_workspace_id()
target_eventhouse_name = 'PowerUtilitiesEH'
target_kql_db_name = 'PowerUtilitiesEH'
source_workspace_id = target_workspace_id 
source_item_id = fabric.resolve_item_id('ReferenceDataLH', 'Lakehouse')

def create_internal_shortcut_in_kql_db(base_url, target_workspace_id, target_kql_db_id, 
                                        target_path, target_shortcut_name, source_workspace_id, 
                                        source_item_id, source_path):
    """Create an internal OneLake shortcut within a KQL Database."""
    
    url = f"{base_url}/v1/workspaces/{target_workspace_id}/items/{target_kql_db_id}/shortcuts"
    
    payload = {
        "path": f"{target_path}",
        "name": f"{target_shortcut_name}",
        "target": {
            "type": "OneLake",
            "oneLake": {
                "workspaceId": f"{source_workspace_id}",
                "itemId": f"{source_item_id}",
                "path": f"{source_path}"
            }
        }
    }
    
    token = notebookutils.credentials.getToken('pbi')
    header = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    try:
        response = requests.post(url, json=payload, headers=header, timeout=30)
        
        try:
            print(response.json())
        except ValueError:
            print(f"Non-JSON response: {response.text[:500]}")
        
        if not response.ok:
            print(f"⚠️ Warning: Shortcut creation returned status {response.status_code}")
            
    except requests.RequestException as e:
        print(f"❌ Error creating shortcut: {e}")
        return None
    
    return response


def get_kusto_query_uri(workspace_id, eventhouse_name):
    """Retrieve the Kusto query service URI for a given Eventhouse."""
    
    eventhouse_id = fabric.resolve_item_id(eventhouse_name)
    client = fabric.FabricRestClient()
    url = f"v1/workspaces/{workspace_id}/eventhouses/{eventhouse_id}"
    
    try:
        response = client.get(url)
        kusto_query_uri = response.json()['properties']['queryServiceUri']
        return kusto_query_uri
    except (KeyError, ValueError) as e:
        print(f"❌ Error parsing queryServiceUri: {e}")
        raise


def exec_kql_command(kusto_query_uri, kql_db_name, kql_command):
    """Execute a KQL management command against a Kusto database."""
    
    token = notebookutils.credentials.getToken(f"{kusto_query_uri}")
    mgmt_url = f"{kusto_query_uri}/v1/rest/mgmt"

    payload = {
        "csl": kql_command,
        "db": kql_db_name
    }
    
    header = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    try:
        response = requests.post(mgmt_url, json=payload, headers=header, timeout=60)
        print(response)
        
        if not response.ok:
            print(f"⚠️ Warning: KQL command returned status {response.status_code}")
            
    except requests.RequestException as e:
        print(f"❌ Error executing KQL command: {e}")
        return None
    
    return response


def create_accelerated_shortcut_in_kql_db(target_workspace_id, target_kql_db_name, 
                                           target_shortcut_name, source_workspace_id, 
                                           source_item_id, source_path):
    """Create OneLake shortcut and accelerated external table in KQL Database."""
    
    base_url = fabric.FabricRestClient().default_base_url
    target_kql_db_id = fabric.resolve_item_id(target_kql_db_name, 'KQLDatabase')
    kusto_query_uri = get_kusto_query_uri(target_workspace_id, target_eventhouse_name)
    
    target_path = 'Shortcut'  # Fixed value for KQL Database

    print(f"Creating shortcut '{target_shortcut_name}' in KQL Database...")
    shortcut_response = create_internal_shortcut_in_kql_db(
        base_url, target_workspace_id, target_kql_db_id, target_path, 
        target_shortcut_name, source_workspace_id, source_item_id, source_path
    )
    
    if shortcut_response is None or not shortcut_response.ok:
        print("❌ Aborting: Shortcut creation failed")
        return None

    # Construct OneLake path
    lakehouse_table_path = notebookutils.lakehouse.getWithProperties("ReferenceDataLH").properties["oneLakeTablesPath"]
    base_path = "/".join(lakehouse_table_path.rstrip("/").split("/")[:-2])
    kql_db_source_path = f"{base_path}/{target_kql_db_id}/{target_path}/{target_shortcut_name}"

    try:
        # Create external table
        print(f"Creating external table '{target_shortcut_name}'...")
        kql_command = f""".create-or-alter external table {target_shortcut_name} kind=delta (h@'{kql_db_source_path};impersonate')"""
        exec_kql_command(kusto_query_uri, target_kql_db_name, kql_command)
        print('✅ External table created')
        
        # Enable query acceleration
        print(f"Enabling query acceleration on '{target_shortcut_name}'...")
        kql_command = f""".alter external table {target_shortcut_name} policy query_acceleration '{{"IsEnabled": true, "Hot": "365.00:00:00", "MaxAge": "01:00:00"}}'"""
        exec_kql_command(kusto_query_uri, target_kql_db_name, kql_command)
        print('✅ Enabled acceleration on external table')
        
    except Exception as e:
        print(f"❌ Error executing KQL command: {str(e)}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create shortcuts for all reference tables
tables = ['feeders', 'meters', 'substations', 'transformers']

for table in tables:
    source_path = f"Tables/{table}" 
    target_shortcut_name = table.capitalize()
    
    print(f"\n{'='*60}")
    print(f"Processing table: {table}")
    print(f"{'='*60}")
    
    create_accelerated_shortcut_in_kql_db(
        target_workspace_id, 
        target_kql_db_name, 
        target_shortcut_name, 
        source_workspace_id, 
        source_item_id, 
        source_path
    )

print("\n✅ All shortcuts created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute a KQL Query to load data into the MeterContextualization table
kusto_query_uri = get_kusto_query_uri(target_workspace_id, target_eventhouse_name)
kql_command = f""".set-or-replace MeterContextualization <| MeterContextualizationFunction()"""
exec_kql_command(kusto_query_uri, target_kql_db_name, kql_command)
print('✅ Loaded data into the MeterContextualiazation table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
