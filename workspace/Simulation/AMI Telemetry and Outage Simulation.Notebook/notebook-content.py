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

# # AMI Telemetry and Outage Simulation
# 
# Simulates realistic smart meter telemetry data with failures, outages, and real-time streaming to Azure Event Hub.
# 
# **Features**:
# - Realistic telemetry: Power consumption, voltage, current, power quality metrics with seasonal/daily patterns
# - Failure simulation: Individual meter failures, battery warnings, tamper detection, last gasp messages
# - Outage simulation: Neighborhood-wide correlated outages triggered by storm data from KQL queries  
# - Real-time streaming: Azure Event Hub integration with batching and error handling
# 
# **Runtime**: Typically runs for 2 hours generating telemetry every minute  
# **Progress**: Status printed every minute during simulation with cycle count and metrics.

# MARKDOWN ********************

# ## 1. Setup and Configuration

# CELL ********************

# Install required packages
%pip install pyspark faker azure-eventhub pandas numpy --quiet
%pip install semantic-link-labs --quiet

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

# Faker for realistic data generation
from faker import Faker
from faker.providers import automotive, internet, phone_number

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from azure.eventhub import EventHubProducerClient, EventData
import sempy.fabric as fabric

# Initialize Faker
fake = Faker()
fake.add_provider(automotive)
fake.add_provider(internet)
fake.add_provider(phone_number)

# Set random seed for reproducible results
random.seed(42)
np.random.seed(42)
Faker.seed(42)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Kusto Query URI for a given eventhouse
def get_kusto_query_uri(eventhouse_name):
    workspace_id = fabric.resolve_workspace_id()
    eventhouse_id = fabric.resolve_item_id('PowerUtilitiesEH')
    client = fabric.FabricRestClient()
    url = f"v1/workspaces/{workspace_id}/eventhouses/{eventhouse_id}"
    response = client.get(url)
    kusto_query_uri = response.json()['properties']['queryServiceUri']
    return kusto_query_uri

kusto_query_uri = get_kusto_query_uri('PowerUtilitiesEH')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get connection string for a given Eventstream
def get_eventstream_connection_string(eventstream_name, eventstream_source_name):
    workspace_id = fabric.resolve_workspace_id()
    
    #Get Eventstream Id
    eventstream_id = fabric.resolve_item_id(eventstream_name)
    
    # Get Source Id
    client = fabric.FabricRestClient()
    url = f"v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/topology"
    response = client.get(url)
    for src in response.json().get("sources", []):
        if src.get("name") == eventstream_source_name and src.get("type") == "CustomEndpoint":
            eventstream_source_id = src.get("id")

    # Get connection string
    url = f"v1/workspaces/{workspace_id}/eventstreams/{eventstream_id}/sources/{eventstream_source_id}/connection"
    response = client.get(url)
    eventstream_connection_string = response.json()['accessKeys']['primaryConnectionString']
    return eventstream_connection_string

eventstream_connection_string = get_eventstream_connection_string(eventstream_name = "AMI_EventStream", eventstream_source_name = "AMISource")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Retrieve Reference Data

# CELL ********************

# Retrieve reference data from Lakehouse tables
try:
    feeders_df = spark.sql("SELECT * FROM ReferenceDataLH.feeders")
    meters_df = spark.sql("SELECT * FROM ReferenceDataLH.meters")
    substations_df = spark.sql("SELECT * FROM ReferenceDataLH.substations")
    transformers_df = spark.sql("SELECT * FROM ReferenceDataLH.transformers")
    
    print(f"✅ Reference data loaded successfully:")
    print(f"   Substations:  {substations_df.count():,}")
    print(f"   Feeders:      {feeders_df.count():,}")
    print(f"   Transformers: {transformers_df.count():,}")
    print(f"   Meters:       {meters_df.count():,}")
except Exception as e:
    print(f"❌ Error loading reference data: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Telemetry Generation Functions
# 
# Realistic load profiles based on meter type, time of day, and season with power quality calculations.

# CELL ********************

import math
import builtins  # Import to access Python's built-in functions
from datetime import datetime, timedelta
from typing import Dict  # Add missing type hint import

def generate_realistic_load_profile(meter_type: str, hour: int, day_of_week: int, season: str) -> float:
    """Generate realistic power consumption based on meter type, time, and season"""
    
    base_loads = {
        "residential": {"summer": 2.5, "winter": 3.2, "spring": 1.8, "fall": 2.0},
        "commercial": {"summer": 15.0, "winter": 12.0, "spring": 10.0, "fall": 11.0},
        "industrial": {"summer": 45.0, "winter": 50.0, "spring": 42.0, "fall": 44.0}
    }
    
    base_load = base_loads[meter_type][season]
    
    if meter_type == "residential":
        # Residential load patterns
        if day_of_week < 5:  # Weekday
            if 6 <= hour <= 8 or 17 <= hour <= 22:  # Morning and evening peaks
                multiplier = 1.4 + random.uniform(-0.2, 0.3)
            elif 9 <= hour <= 16:  # Daytime low
                multiplier = 0.6 + random.uniform(-0.1, 0.2)
            elif 23 <= hour or hour <= 5:  # Night low
                multiplier = 0.3 + random.uniform(-0.1, 0.1)
            else:
                multiplier = 1.0 + random.uniform(-0.2, 0.2)
        else:  # Weekend
            if 8 <= hour <= 11 or 18 <= hour <= 21:  # Weekend peaks
                multiplier = 1.2 + random.uniform(-0.1, 0.2)
            elif 12 <= hour <= 17:  # Afternoon
                multiplier = 1.0 + random.uniform(-0.15, 0.15)
            else:
                multiplier = 0.7 + random.uniform(-0.1, 0.1)
    
    elif meter_type == "commercial":
        # Commercial load patterns
        if day_of_week < 5:  # Weekday
            if 7 <= hour <= 18:  # Business hours
                multiplier = 1.0 + random.uniform(-0.1, 0.2)
            else:  # After hours
                multiplier = 0.3 + random.uniform(-0.05, 0.1)
        else:  # Weekend
            multiplier = 0.2 + random.uniform(-0.05, 0.1)
    
    else:  # Industrial
        # Industrial - more consistent but with shifts
        if day_of_week < 5:  # Weekday
            if 6 <= hour <= 22:  # Operating hours
                multiplier = 0.9 + random.uniform(-0.1, 0.2)
            else:  # Night shift reduced
                multiplier = 0.7 + random.uniform(-0.1, 0.1)
        else:  # Weekend - reduced operations
            multiplier = 0.6 + random.uniform(-0.1, 0.1)
    
    # Add seasonal variations
    if season == "summer" and 12 <= hour <= 18:  # AC load
        multiplier *= 1.3
    elif season == "winter" and (6 <= hour <= 9 or 17 <= hour <= 21):  # Heating
        multiplier *= 1.2
    
    return base_load * multiplier

def calculate_electrical_parameters(power_kw: float, voltage_nominal: int, phases: int, power_factor: float = None) -> Dict:
    """Calculate realistic electrical parameters based on power consumption"""
    
    if power_factor is None or power_factor == 0:
        # Realistic power factors by load type
        if power_kw < 5:  # Residential
            power_factor = 0.95 + random.uniform(-0.05, 0.03)
        elif power_kw < 25:  # Small commercial
            power_factor = 0.92 + random.uniform(-0.08, 0.05)
        else:  # Large commercial/industrial
            power_factor = 0.88 + random.uniform(-0.10, 0.08)
        
        # Use Python's built-in min and max functions
        power_factor = builtins.max(0.75, builtins.min(0.99, power_factor))  # Realistic bounds
    
    # Calculate apparent power
    apparent_power_kva = power_kw / power_factor
    
    # Calculate reactive power
    reactive_power_kvar = math.sqrt(apparent_power_kva**2 - power_kw**2)
    
    # Calculate current (simplified for demonstration)
    if phases == 1:
        current_amps = (apparent_power_kva * 1000) / voltage_nominal
    else:  # 3-phase
        current_amps = (apparent_power_kva * 1000) / (voltage_nominal * math.sqrt(3))
    
    # Add realistic voltage variations (±5% of nominal)
    voltage_actual = voltage_nominal * (1 + random.uniform(-0.05, 0.05))
    
    # Power quality metrics
    thd_voltage = random.uniform(0.5, 3.0)  # Total Harmonic Distortion
    thd_current = random.uniform(1.0, 8.0)
    frequency = 60.0 + random.uniform(-0.1, 0.1)  # Grid frequency variation
    
    return {
        "active_power_kw": builtins.round(power_kw, 3),
        "reactive_power_kvar": builtins.round(reactive_power_kvar, 3),
        "apparent_power_kva": builtins.round(apparent_power_kva, 3),
        "current_amps": builtins.round(current_amps, 2),
        "voltage_volts": builtins.round(voltage_actual, 1),
        "power_factor": builtins.round(power_factor, 3),
        "frequency_hz": builtins.round(frequency, 2),
        "thd_voltage_percent": builtins.round(thd_voltage, 2),
        "thd_current_percent": builtins.round(thd_current, 2)
    }

def generate_telemetry_reading(meter: Dict, timestamp: datetime, season: str) -> Dict:
    """Generate a single telemetry reading for a meter"""
    
    hour = timestamp.hour
    day_of_week = timestamp.weekday()
    
    # Generate realistic power consumption
    power_kw = generate_realistic_load_profile(meter["meter_type"], hour, day_of_week, season)
    
    # Calculate electrical parameters
    electrical_params = calculate_electrical_parameters(
        power_kw, meter["voltage"], meter["phases"], 0
    )
    
    # Energy accumulation (simplified)
    energy_kwh_delivered = power_kw * (15/60)  # 15-minute interval
    energy_kwh_received = 0  # Assume no generation for most meters
    
    # Occasionally simulate solar generation for residential meters
    if meter["meter_type"] == "residential" and random.random() < 0.15:  # 15% have solar
        if 8 <= hour <= 17 and season in ["spring", "summer"]:  # Daylight hours
            solar_generation = random.uniform(0.5, 4.0)  # kW
            energy_kwh_received = solar_generation * (15/60)
            if solar_generation > power_kw:
                electrical_params["active_power_kw"] = -(solar_generation - power_kw)  # Net export
    
    # Device status information
    signal_strength = random.randint(-85, -45)  # dBm
    battery_voltage = 3.6 + random.uniform(-0.2, 0.1) if meter["communication_type"] in ["RF Mesh", "Cellular"] else None
    temperature_c = random.uniform(15, 45)  # Internal temperature
    
    # Tamper and alarm status
    tamper_detected = random.random() < 0.001  # 0.1% chance
    power_outage = random.random() < 0.002  # 0.2% chance
    
    telemetry = {
        "meter_id": meter["meter_id"],
        "timestamp": timestamp.isoformat(),
        "message_type": "interval_reading",
        "interval_minutes": 1,
        
        # Power measurements
        **electrical_params,
        
        # Energy measurements
        "energy_delivered_kwh": builtins.round(energy_kwh_delivered, 4),
        "energy_received_kwh": builtins.round(energy_kwh_received, 4),
        "energy_net_kwh": builtins.round(energy_kwh_delivered - energy_kwh_received, 4),
        
        # Cumulative energy (simplified simulation)
        "cumulative_energy_kwh": builtins.round(random.uniform(1000, 50000), 2),
        
        # Device status
        "signal_strength_dbm": signal_strength,
        "battery_voltage": battery_voltage,
        "internal_temperature_c": builtins.round(temperature_c, 1),
        
        # Status flags
        "tamper_detected": tamper_detected,
        "power_outage": power_outage,
        "communication_error": False,
        "meter_error": False,
               
        # Message metadata
        "sequence_number": random.randint(1000, 9999),
        "data_quality": "good",
        "firmware_version": meter["firmware_version"]
    }
    
    return telemetry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Failure Simulation Classes
# 
# Individual meter failures with last gasp messages and battery warnings.

# CELL ********************

import builtins  # Import to access Python's built-in functions

class MeterFailureSimulator:
    """Simulate various meter failure scenarios"""
    
    def __init__(self):
        self.failed_meters = set()
        self.battery_low_meters = set()
        self.communication_error_meters = set()
        self.tamper_detected_meters = set()
        
        # Failure probabilities per hour
        self.failure_rates = {
            "battery_failure": 0.00001,      # Very rare
            "communication_failure": 0.0001,  # Uncommon
            "tamper_detection": 0.000005,     # Very rare
            "meter_malfunction": 0.000002,    # Extremely rare
            "power_supply_failure": 0.00002   # Rare
        }
    
    def generate_last_gasp_message(self, meter: Dict, failure_type: str, timestamp: datetime) -> Dict:
        """Generate a last gasp message for a failing meter"""
        
        # Battery information for last gasp
        if failure_type == "battery_failure":
            battery_voltage = random.uniform(2.8, 3.1)  # Critical battery level
            alert_code = "BATT_LOW"
            description = "Battery voltage critically low"
        elif failure_type == "power_supply_failure":
            battery_voltage = random.uniform(3.0, 3.4)  # Using backup battery
            alert_code = "PWR_FAIL"
            description = "Primary power supply failure, running on backup battery"
        elif failure_type == "communication_failure":
            battery_voltage = random.uniform(3.2, 3.6) if meter["communication_type"] in ["RF Mesh", "Cellular"] else None
            alert_code = "COMM_FAIL"
            description = "Communication module failure"
        elif failure_type == "tamper_detection":
            battery_voltage = random.uniform(3.3, 3.6) if meter["communication_type"] in ["RF Mesh", "Cellular"] else None
            alert_code = "TAMPER"
            description = "Meter tamper detected"
        else:  # meter_malfunction
            battery_voltage = random.uniform(3.0, 3.6) if meter["communication_type"] in ["RF Mesh", "Cellular"] else None
            alert_code = "METER_ERR"
            description = "Meter malfunction detected"
        
        last_gasp = {
            "meter_id": meter["meter_id"],
            "timestamp": timestamp.isoformat(),
            "message_type": "last_gasp",
            "alert_code": alert_code,
            "alert_description": description,
            "failure_type": failure_type,
            
            # Final readings before failure
            "final_energy_reading_kwh": builtins.round(random.uniform(1000, 50000), 2),
            "signal_strength_dbm": random.randint(-95, -85),  # Weak signal
            "battery_voltage": battery_voltage,
            "internal_temperature_c": builtins.round(random.uniform(20, 60), 1),
            
            # Diagnostic information
            "error_codes": [random.randint(1000, 9999) for _ in range(random.randint(1, 3))],
            "retry_attempts": random.randint(3, 10),
            "last_successful_transmission": (timestamp - timedelta(hours=random.randint(1, 48))).isoformat(),
            
            # Metadata
            "sequence_number": random.randint(1000, 9999),
            "firmware_version": meter["firmware_version"],
            "priority": "high",
            "requires_investigation": True
        }
        
        return last_gasp
    
    def simulate_individual_failures(self, meters_list: List[Dict], timestamp: datetime) -> List[Dict]:
        """Simulate random individual meter failures"""
        
        failure_messages = []
        
        for meter in meters_list:
            meter_id = meter["meter_id"]
            
            # Skip if meter already failed
            if meter_id in self.failed_meters:
                continue
            
            # Check for various failure types
            for failure_type, rate in self.failure_rates.items():
                if random.random() < rate:
                    # Generate last gasp message
                    last_gasp = self.generate_last_gasp_message(meter, failure_type, timestamp)
                    failure_messages.append(last_gasp)
                    
                    # Mark meter as failed (except for tamper which might be temporary)
                    if failure_type != "tamper_detection":
                        self.failed_meters.add(meter_id)
                    else:
                        self.tamper_detected_meters.add(meter_id)
                    
                    print(f"FAILURE: {meter_id} - {failure_type} at {timestamp}")
                    break  # Only one failure type per meter at a time
        
        return failure_messages
    
    def generate_battery_low_warning(self, meter: Dict, timestamp: datetime) -> Dict:
        """Generate battery low warning (before complete failure)"""
        
        warning = {
            "meter_id": meter["meter_id"],
            "timestamp": timestamp.isoformat(),
            "message_type": "battery_warning",
            "alert_code": "BATT_WARN",
            "alert_description": "Battery voltage low - maintenance required",
            
            "battery_voltage": builtins.round(random.uniform(3.1, 3.3), 2),
            "estimated_remaining_hours": random.randint(24, 168),  # 1-7 days
            "signal_strength_dbm": random.randint(-80, -60),
            "internal_temperature_c": builtins.round(random.uniform(20, 45), 1),
           
            "sequence_number": random.randint(1000, 9999),
            "firmware_version": meter["firmware_version"],
            "priority": "medium",
            "requires_maintenance": True
        }
        
        return warning
    
    def simulate_battery_warnings(self, meters_list: List[Dict], timestamp: datetime) -> List[Dict]:
        """Simulate battery low warnings"""
        
        warning_messages = []
        battery_warning_rate = 0.0002  # Higher rate for warnings than failures
        
        for meter in meters_list:
            meter_id = meter["meter_id"]
            
            # Only for wireless meters and those not already failed
            if (meter["communication_type"] in ["RF Mesh", "Cellular"] and 
                meter_id not in self.failed_meters and 
                meter_id not in self.battery_low_meters):
                
                if random.random() < battery_warning_rate:
                    warning = self.generate_battery_low_warning(meter, timestamp)
                    warning_messages.append(warning)
                    self.battery_low_meters.add(meter_id)
                    print(f"WARNING: {meter_id} - battery low at {timestamp}")
        
        return warning_messages

# Initialize failure simulator
failure_simulator = MeterFailureSimulator()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Outage Simulation Classes
# 
# Correlated failures affecting multiple meters (feeder/transformer outages triggered by storms).

# CELL ********************

class NeighborhoodOutageSimulator:
    """Simulate correlated outages affecting multiple meters"""
    
    def __init__(self):
        self.active_outages = {}  # feeder_id -> outage_info
        self.outage_history = []
    
    def generate_outage_event(self, outage_type: str, affected_component: str, 
                            affected_meters: List[Dict], timestamp: datetime) -> Dict:
        """Generate an outage event affecting multiple meters"""

        outage_event = {
            "outage_id": str(uuid.uuid4()),
            "outage_type": outage_type,
            "start_time": timestamp.isoformat(),
            "priority": "high"
        }
        
        return outage_event
    
    def generate_outage_last_gasp_messages(self, affected_meters: List[Dict], 
                                         outage_event: Dict, timestamp: datetime) -> List[Dict]:
        """Generate last gasp messages for all meters affected by outage"""
        
        messages = []
        
        for meter in affected_meters:
            # Stagger the last gasp messages slightly to simulate realistic timing
            gasp_timestamp = timestamp + timedelta(seconds=random.randint(0, 30))
            
            last_gasp = {
                "meter_id": meter["meter_id"],
                "timestamp": gasp_timestamp.isoformat(),
                "message_type": "outage_last_gasp",
                "outage_id": outage_event["outage_id"],
                "alert_code": "POWER_OUT",
                "alert_description": "Power outage detected",
                
                # Power information at time of outage
                "voltage_at_outage": 0.0,  # No voltage during outage
                "last_voltage_reading": builtins.round(meter["voltage"] * (1 + random.uniform(-0.02, 0.02)), 1),
                "power_at_outage": 0.0,
                
                # Battery backup information (for wireless meters)
                "battery_voltage": (builtins.round(random.uniform(3.2, 3.6), 2) 
                                  if meter["communication_type"] in ["RF Mesh", "Cellular"] else None),
                "backup_power_available": meter["communication_type"] in ["RF Mesh", "Cellular"],
                "estimated_backup_hours": (random.randint(12, 72) 
                                         if meter["communication_type"] in ["RF Mesh", "Cellular"] else 0),
                
                # Outage correlation information
                "outage_type": outage_event["outage_type"],
                "correlated_outage": True,
                
                # Metadata
                "sequence_number": random.randint(1000, 9999),
                "firmware_version": meter["firmware_version"],
                "priority": "high",
                "automatic_restoration_expected": True
            }
            
            messages.append(last_gasp)
        
        return messages
    
    def simulate_feeder_outage(self, meters_df: DataFrame, affected_feeders_df: DataFrame,
                             timestamp: datetime) -> Tuple[Dict, List[Dict]]:
        """Simulate an outage affecting an entire feeder"""
        affected_feeders = [row.feeder_id for row in affected_feeders_df.collect()]

        # Select a random feeder
        for affected_feeder in affected_feeders:  
            #Get all transformers on this feeder
            affected_transformers = transformers_df.filter(col("feeder_id") == affected_feeder)
            affected_transformers = [row.transformer_id for row in affected_transformers.collect()]

            # Get all meters on this feeder
            affected_meters = meters_df.filter(col("transformer_id").isin(affected_transformers)).collect()
            affected_meters = [meter.asDict() for meter in affected_meters]
            
            # Generate outage event
            outage_event = self.generate_outage_event("feeder_outage", affected_feeder, 
                                                    affected_meters, timestamp)
        
            # Generate last gasp messages
            last_gasp_messages = self.generate_outage_last_gasp_messages(
                affected_meters, outage_event, timestamp)
            
            # Track active outage
            self.active_outages[affected_feeder] = outage_event
            self.outage_history.append(outage_event)
            
            print(f"Generated {len(last_gasp_messages)} last gasp messages")

            return outage_event, last_gasp_messages
    
    def simulate_transformer_outage(self, meters_df: DataFrame, affected_transformers_df: DataFrame, 
                                  timestamp: datetime) -> Tuple[Dict, List[Dict]]:
        """Simulate an outage affecting meters on one transformer"""
        affected_transformers = [row.transformer_id for row in affected_transformers_df.collect()]

        for affected_transformer in affected_transformers:           
            # Get all meters on this transformer
            affected_meters = meters_df.filter(col("transformer_id") == affected_transformer).collect()
            affected_meters = [meter.asDict() for meter in affected_meters]
            
            # Generate outage event
            outage_event = self.generate_outage_event("transformer_outage", affected_transformer, 
                                                    affected_meters, timestamp)
            
            # Generate last gasp messages
            last_gasp_messages = self.generate_outage_last_gasp_messages(
                affected_meters, outage_event, timestamp)
            
            # Track active outage
            self.active_outages[affected_transformer] = outage_event
            self.outage_history.append(outage_event)
            
            print(f"Generated {len(last_gasp_messages)} last gasp messages")

            return outage_event, last_gasp_messages

# Initialize neighborhood outage simulator
outage_simulator = NeighborhoodOutageSimulator()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Azure Event Hub Integration
# 
# Connection setup and message transmission with batching.

# CELL ********************

# Configure Azure Event Hub connection parameters
AZURE_EVENTHUB_CONNECTION_STRING = eventstream_connection_string

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class AMIEventHubClient:
    """Azure Event Hub client for AMI telemetry data"""
    
    def __init__(self, connection_string: str = None):
        """Initialize Event Hub client
        
        Args:
            connection_string: Azure Event Hub connection string
        """
        self.connection_string = connection_string
        self.producer_client = None
        self.message_count = 0
        self.error_count = 0
        
        self.producer_client = EventHubProducerClient.from_connection_string(conn_str=self.connection_string)
    
    def format_message_for_eventhub(self, telemetry_data: Dict) -> Dict:
        """Format telemetry data for Event Hub transmission"""
        
        # Add Event Hub specific metadata
        formatted_message = {
            "messageId": str(uuid.uuid4()),
            "messageType": telemetry_data.get("message_type", "unknown"),
            "deviceId": telemetry_data["meter_id"],
            "timestamp": telemetry_data["timestamp"],
            "version": "1.1",
            "source": "AMI_Capture",
            
            # Core telemetry payload
            "telemetry": telemetry_data,
            
            # Message routing information
            "partitionKey": telemetry_data["meter_id"],  # Route by meter ID
            "priority": telemetry_data.get("priority", "normal"),
            
            # Correlation information for outages
            "correlationId": telemetry_data.get("outage_id", None)
        }
        
        return formatted_message
    
    def send_message(self, telemetry_data: Dict) -> bool:
        """Send a single telemetry message to Event Hub"""
        
        try:
            formatted_message = self.format_message_for_eventhub(telemetry_data)
            message_json = json.dumps(formatted_message)
            
            if self.producer_client:
                # Send to actual Azure Event Hub
                event_data = EventData(message_json)
                
                # Set partition key for load balancing
                event_data.properties = {
                    "meter_id": telemetry_data["meter_id"],
                    "message_type": telemetry_data.get("message_type", "unknown"),
                    "priority": telemetry_data.get("priority", "normal")
                }
                
                with self.producer_client:
                    event_data_batch = self.producer_client.create_batch()
                    event_data_batch.add(event_data)
                    self.producer_client.send_batch(event_data_batch)
                
                self.message_count += 1
                return True
            else:
                # Simulation mode - just log the message
                print(f"SIMULATION: Would send message from {telemetry_data['meter_id']} " +
                      f"({telemetry_data.get('message_type', 'unknown')}) to Event Hub")
                self.message_count += 1
                return True
                
        except Exception as e:
            print(f"Error sending message: {e}")
            self.error_count += 1
            return False
    
    def send_batch(self, telemetry_batch: List[Dict]) -> Dict:
        """Send a batch of telemetry messages to Event Hub with error handling"""
        
        success_count = 0
        failure_count = 0
        
        if not telemetry_batch:
            return {"total_messages": 0, "successful": 0, "failed": 0, "success_rate": 1.0}
        
        try:
            if self.producer_client:
                # Send to actual Azure Event Hub in batches
                formatted_messages = [self.format_message_for_eventhub(data) for data in telemetry_batch]
                
                with self.producer_client:
                    event_data_batch = self.producer_client.create_batch()
                    
                    for telemetry_data, formatted_msg in zip(telemetry_batch, formatted_messages):
                        try:
                            message_json = json.dumps(formatted_msg)
                            event_data = EventData(message_json)
                            
                            # Set partition key for load balancing
                            event_data.properties = {
                                "meter_id": telemetry_data["meter_id"],
                                "message_type": telemetry_data.get("message_type", "unknown")
                            }
                            
                            event_data_batch.add(event_data)
                            success_count += 1
                            
                        except Exception as e:
                            print(f"⚠️  Error adding message to batch: {str(e)[:100]}")
                            failure_count += 1
                    
                    # Send batch if not empty
                    if event_data_batch:
                        try:
                            self.producer_client.send_batch(event_data_batch)
                            self.message_count += success_count
                        except Exception as e:
                            print(f"❌ Error sending batch to Event Hub: {str(e)[:150]}")
                            # Mark all as failed
                            failure_count = len(telemetry_batch)
                            success_count = 0
            else:
                # Simulation mode - all succeed
                success_count = len(telemetry_batch)
                        
        except Exception as e:
            print(f"❌ Critical error in send_batch: {str(e)[:150]}")
            failure_count = len(telemetry_batch)
            success_count = 0
            self.error_count += failure_count
        
        return {
            "total_messages": len(telemetry_batch),
            "successful": success_count,
            "failed": failure_count,
            "success_rate": success_count / len(telemetry_batch) if telemetry_batch else 0
        }
    
    def get_stats(self) -> Dict:
        """Get transmission statistics"""
        return {
            "total_messages_sent": self.message_count,
            "total_errors": self.error_count,
            "success_rate": (self.message_count / (self.message_count + self.error_count) 
                           if (self.message_count + self.error_count) > 0 else 0),
            "event_hub_connected": self.producer_client is not None
        }
    
    def close(self):
        """Close the Event Hub client"""
        if self.producer_client:
            self.producer_client.close()

# Configuration for Azure Event Hub
EVENT_HUB_CONFIG = {
    "connection_string": AZURE_EVENTHUB_CONNECTION_STRING,
    "batch_size": 500,  # Messages per batch
    "send_interval_seconds": 1,  # Send every 5 seconds
    "retry_attempts": 3,
    "timeout_seconds": 1
}

print("Azure Event Hub Configuration:")
for key, value in EVENT_HUB_CONFIG.items():
    if key == "connection_string" and value:
        print(f"  {key}: [CONFIGURED]")
    else:
        print(f"  {key}: {value}")

# Initialize Event Hub client
event_hub_client = AMIEventHubClient(
    connection_string=EVENT_HUB_CONFIG["connection_string"],
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Telemetry Orchestration
# 
# Coordinates telemetry generation, failure simulation, and Event Hub transmission.

# CELL ********************

import time
from concurrent.futures import ThreadPoolExecutor
import threading

class AMITelemetryOrchestrator:
    """Orchestrates the complete AMI telemetry simulation and transmission"""
    
    def __init__(self, meters_df: DataFrame, event_hub_client: AMIEventHubClient,
                 failure_simulator: MeterFailureSimulator, outage_simulator: NeighborhoodOutageSimulator):
        self.meters_df = meters_df
        self.event_hub_client = event_hub_client
        self.failure_simulator = failure_simulator
        self.outage_simulator = outage_simulator
        
        self.simulation_running = False
        self.stats = {
            "total_readings_generated": 0,
            "total_failures": 0,
            "total_outages": 0,
            "total_messages_sent": 0,
            "start_time": None,
            "last_batch_time": None
        }
    
    def determine_season(self, timestamp: datetime) -> str:
        """Determine season based on timestamp"""
        month = timestamp.month
        if month in [12, 1, 2]:
            return "winter"
        elif month in [3, 4, 5]:
            return "spring"
        elif month in [6, 7, 8]:
            return "summer"
        else:
            return "fall"
    
    def generate_telemetry_batch(self, timestamp: datetime, meters_sample: List[Dict] = None) -> List[Dict]:
        """Generate a batch of telemetry readings for all active meters"""
        
        if meters_sample is None:
            # Get all active meters (not failed)
            all_meters = self.meters_df.collect()
            active_meters = [m.asDict() for m in all_meters 
                           if m.meter_id not in self.failure_simulator.failed_meters]
        else:
            active_meters = meters_sample
        
        season = self.determine_season(timestamp)
        telemetry_batch = []
        
        for meter in active_meters:
            try:
                reading = generate_telemetry_reading(meter, timestamp, season)
                telemetry_batch.append(reading)
                self.stats["total_readings_generated"] += 1
            except Exception as e:
                print(f"Error generating reading for meter {meter['meter_id']}: {e}")
        
        return telemetry_batch
    
    def simulate_failures_and_outages(self, timestamp: datetime) -> List[Dict]:
        """Simulate various failure scenarios and return failure messages"""
        
        failure_messages = []
        
        # Get active meters for failure simulation
        all_meters = self.meters_df.collect()
        active_meters = [m.asDict() for m in all_meters 
                        if m.meter_id not in self.failure_simulator.failed_meters]
        
        # Simulate individual meter failures
        individual_failures = self.failure_simulator.simulate_individual_failures(active_meters, timestamp)
        failure_messages.extend(individual_failures)
        self.stats["total_failures"] += len(individual_failures)
        
        # Simulate battery warnings
        battery_warnings = self.failure_simulator.simulate_battery_warnings(active_meters, timestamp)
        failure_messages.extend(battery_warnings)
        
        # Simulate neighborhood outages triggered by storm data
        # Query KQL for transformers and feeders affected by storms
        
        affected_transformers_df = None
        affected_feeders_df = None
        
        try:
            kusto_uri = kusto_query_uri
            kusto_database = "PowerUtilitiesEH"
            kusto_access_token = notebookutils.credentials.getToken('kusto')

            # Query for affected transformers
            transformers_query = 'GetTransformersAffectedByLatestStorm(10)'
            affected_transformers_df = spark.read\
                .format("com.microsoft.kusto.spark.synapse.datasource")\
                .option("accessToken", kusto_access_token)\
                .option("kustoCluster", kusto_uri)\
                .option("kustoDatabase", kusto_database)\
                .option("kustoQuery", transformers_query).load()

            # Query for affected feeders
            feeders_query = 'GetFeedersAffectedByLatestStorm(5)'
            affected_feeders_df = spark.read\
                .format("com.microsoft.kusto.spark.synapse.datasource")\
                .option("accessToken", kusto_access_token)\
                .option("kustoCluster", kusto_uri)\
                .option("kustoDatabase", kusto_database)\
                .option("kustoQuery", feeders_query).load()
                
        except Exception as e:
            print(f"⚠️  Warning: Could not query storm data from KQL: {str(e)[:100]}")
            # Continue without storm-triggered outages

        if affected_feeders_df and affected_feeders_df.count() > 0:
            outage_type = "feeder_outage"
            outage_event, outage_messages = self.outage_simulator.simulate_feeder_outage(
                    self.meters_df, affected_feeders_df, timestamp)
            
            failure_messages.extend(outage_messages)
            self.stats["total_outages"] += 1
            
            print(f"OUTAGE SIMULATED: {outage_event['outage_type']}")        
        
        if affected_transformers_df and affected_transformers_df.count() > 0:
            outage_type = "transformer_outage"
            outage_event, outage_messages = self.outage_simulator.simulate_transformer_outage(
                    self.meters_df, affected_transformers_df, timestamp)
            
            failure_messages.extend(outage_messages)
            self.stats["total_outages"] += 1
            
            print(f"OUTAGE SIMULATED: {outage_event['outage_type']}")
        
        return failure_messages
    
    def send_telemetry_batch(self, telemetry_batch: List[Dict], failure_messages: List[Dict]) -> Dict:
        """Send telemetry and failure messages to Event Hub"""
        
        all_messages = telemetry_batch + failure_messages
        
        if not all_messages:
            return {"total_messages": 0, "successful": 0, "failed": 0}
        
        # Send in smaller batches to avoid Event Hub limits
        batch_size = EVENT_HUB_CONFIG["batch_size"]
        total_results = {"total_messages": 0, "successful": 0, "failed": 0}
        
        for i in range(0, len(all_messages), batch_size):
            batch = all_messages[i:i + batch_size]
            result = self.event_hub_client.send_batch(batch)
            
            total_results["total_messages"] += result["total_messages"]
            total_results["successful"] += result["successful"]
            total_results["failed"] += result["failed"]
            
            # Small delay between batches to avoid overwhelming Event Hub
            if len(all_messages) > batch_size:
                time.sleep(0.1)
        
        self.stats["total_messages_sent"] += total_results["successful"]
        return total_results
    
    def run_simulation_cycle(self, timestamp: datetime, sample_size: int = None) -> Dict:
        """Run one complete simulation cycle"""
        
        cycle_start = time.time()
        
        # Sample meters if requested (for performance testing)
        meters_sample = None
        if sample_size:
            all_meters = self.meters_df.collect()
            active_meters = [m.asDict() for m in all_meters 
                           if m.meter_id not in self.failure_simulator.failed_meters]
            meters_sample = random.sample(active_meters, builtins.min(sample_size, len(active_meters)))
        
        # Generate telemetry batch
        telemetry_batch = self.generate_telemetry_batch(timestamp, meters_sample)
        
        # Simulate failures and outages
        failure_messages = self.simulate_failures_and_outages(timestamp)
        
        # Send to Event Hub
        send_result = self.send_telemetry_batch(telemetry_batch, failure_messages)
        
        cycle_time = time.time() - cycle_start
        
        result = {
            "timestamp": timestamp.isoformat(),
            "cycle_duration_seconds": builtins.round(cycle_time, 2),
            "telemetry_readings": len(telemetry_batch),
            "failure_messages": len(failure_messages),
            "transmission_result": send_result,
            "active_meters": len(telemetry_batch) if telemetry_batch else 0,
            "failed_meters": len(self.failure_simulator.failed_meters)
        }
        
        self.stats["last_batch_time"] = timestamp
        return result
    
    def run_continuous_simulation(self, duration_minutes: int = 60, 
                                interval_minutes: int = 1, sample_size: int = None):
        """Run continuous telemetry simulation with progress tracking"""
        
        print(f"\n{'='*70}")
        print(f"STARTING CONTINUOUS AMI TELEMETRY SIMULATION")
        print(f"{'='*70}")
        print(f"Duration: {duration_minutes} minutes ({duration_minutes/60:.1f} hours)")
        print(f"Interval: {interval_minutes} minute(s)")
        print(f"Sample size: {sample_size or 'All meters'}")
        print(f"Expected cycles: {duration_minutes // interval_minutes}")
        print(f"{'='*70}\n")

        
        self.simulation_running = True
        self.stats["start_time"] = datetime.now()
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        current_time = start_time
        
        cycle_count = 0
        expected_cycles = duration_minutes // interval_minutes
        
        try:
            while current_time < end_time and self.simulation_running:
                cycle_count += 1
                cycle_start_time = time.time()
                
                # Progress indicator with percentage
                progress_pct = (cycle_count / expected_cycles) * 100
                elapsed = datetime.now() - start_time
                
                print(f"\n{'─'*70}")
                print(f"📊 CYCLE {cycle_count}/{expected_cycles} ({progress_pct:.1f}% complete)")
                print(f"⏱️  {current_time.strftime('%Y-%m-%d %H:%M:%S')} | Elapsed: {str(elapsed).split('.')[0]}")
                
                # Run simulation cycle
                try:
                    result = self.run_simulation_cycle(current_time, sample_size)
                    
                    # Print results with emojis for better readability
                    print(f"✅ Generated {result['telemetry_readings']:,} telemetry readings")
                    print(f"⚠️  Generated {result['failure_messages']:,} failure/outage messages")
                   
                    # Send success summary
                    tx_result = result['transmission_result']
                    if tx_result['total_messages'] > 0:
                        success_rate = (tx_result['successful'] / tx_result['total_messages']) * 100
                    
                except Exception as e:
                    print(f"❌ ERROR in cycle {cycle_count}: {str(e)}")
                    # Continue to next cycle despite error
                
                # Move to next interval
                current_time += timedelta(minutes=interval_minutes)
                
                # Sleep until next interval (adjusted for processing time)
                if self.simulation_running and current_time < end_time:
                    cycle_elapsed = time.time() - cycle_start_time
                    sleep_seconds = builtins.max(0, interval_minutes * 60 - cycle_elapsed)
                    
                    if sleep_seconds > 5:
                        remaining_time = end_time - datetime.now()
                        print(f"⏸️  Waiting {sleep_seconds:.1f}s until next cycle... (ETA: {str(remaining_time).split('.')[0]} remaining)")
                        time.sleep(sleep_seconds)
                
        except KeyboardInterrupt:
            print("\n\n⚠️  Simulation interrupted by user")
        except Exception as e:
            print(f"\n\n❌ Simulation error: {str(e)}")
        finally:
            self.simulation_running = False
            
        print(f"\n{'='*70}")
        print(f"✅ SIMULATION COMPLETED after {cycle_count} cycles")
        print(f"{'='*70}")
        self.print_simulation_summary()
    
    def print_simulation_summary(self):
        """Print summary statistics"""
        print("\\n" + "="*50)
        print("AMI TELEMETRY SIMULATION SUMMARY")
        print("="*50)
        
        if self.stats["start_time"]:
            duration = datetime.now() - self.stats["start_time"]
            print(f"Simulation duration: {duration}")
        
        print(f"Total telemetry readings generated: {self.stats['total_readings_generated']:,}")
        print(f"Total failure events: {self.stats['total_failures']}")
        print(f"Total outage events: {self.stats['total_outages']}")
        print(f"Total messages sent to Event Hub: {self.stats['total_messages_sent']:,}")
        print(f"Currently failed meters: {len(self.failure_simulator.failed_meters)}")
        
        # Event Hub client stats
        eh_stats = self.event_hub_client.get_stats()
        print(f"Event Hub success rate: {eh_stats['success_rate']:.2%}")
        print(f"Event Hub connected: {eh_stats['event_hub_connected']}")

# Initialize the orchestrator
orchestrator = AMITelemetryOrchestrator(
    meters_df=meters_df,
    event_hub_client=event_hub_client,
    failure_simulator=failure_simulator,
    outage_simulator=outage_simulator
)

print("AMI Telemetry Orchestrator initialized successfully!")
print(f"Ready to simulate telemetry from {meters_df.count()} meters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Run Simulation
# 
# Execute the complete 2-hour simulation with progress tracking every minute.

# CELL ********************

# Run the simulation for 2 hours generating telemetry every minute
print("\n🚀 Starting extended 2-hour simulation...")
print("📝 Progress will be printed every minute")

def run_extended_simulation(duration_hours: float = 2.0, sample_percentage: float = 1.0):
    """Run an extended simulation with full monitoring"""
    
    total_meters = meters_df.count()
    sample_size = builtins.max(10, int(total_meters * sample_percentage)) if sample_percentage < 1.0 else None
    
    print(f"  Duration: {duration_hours} hours")
    print(f"  Meters: {sample_size or total_meters:,} ({sample_percentage:.0%} of total)")
    
    # Run the simulation with progress tracking
    orchestrator.run_continuous_simulation(
        duration_minutes=int(duration_hours * 60),
        interval_minutes=1,
        sample_size=sample_size
    )
    
    # Final summary
    orchestrator.print_simulation_summary()

# Execute the 2-hour simulation
try:
    run_extended_simulation(duration_hours=2.0, sample_percentage=1.0)
except KeyboardInterrupt:
    print("\n⚠️  Simulation stopped by user")
except Exception as e:
    print(f"\n❌ Simulation error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Summary and Cleanup

# CELL ********************

# Display final statistics
print("\n" + "="*70)
print("AMI TELEMETRY SIMULATION - FINAL SUMMARY")
print("="*70)

try:
    print(f"\n📊 Reference Data:")
    print(f"   Meters configured: {meters_df.count():,}")
    print(f"   Substations: {substations_df.count()}")
    print(f"   Feeders: {feeders_df.count()}")
    print(f"   Transformers: {transformers_df.count()}")

    # Meter distribution
    print(f"\n📈 Meter Distribution:")
    meter_distribution = meters_df.groupBy("meter_type").count().collect()
    for row in meter_distribution:
        pct = (row['count'] / meters_df.count()) * 100
        print(f"   {row['meter_type']:15s}: {row['count']:5,} ({pct:5.1f}%)")

    # Event Hub stats
    eh_final_stats = event_hub_client.get_stats()
    print(f"\n📡 Event Hub Statistics:")
    print(f"   Messages sent: {eh_final_stats['total_messages_sent']:,}")
    print(f"   Errors: {eh_final_stats['total_errors']}")
    print(f"   Success rate: {eh_final_stats['success_rate']:.2%}")

    print(f"\n✅ Simulation setup complete and ready for reuse!")
    
except Exception as e:
    print(f"\n⚠️  Error displaying summary: {e}")

# Cleanup resources
try:
    event_hub_client.close()
    print("\n🧹 Event Hub client closed")
except Exception as e:
    print(f"\n⚠️  Error closing Event Hub client: {e}")

print("="*70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
