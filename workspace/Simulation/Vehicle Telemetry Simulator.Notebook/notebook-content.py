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

# # Vehicle Telemetry Simulator
# 
# Simulates realistic vehicle sensor telemetry from a fleet following road networks with correlated parameters.
# 
# **Features**: Multi-vehicle fleet simulation with route following, realistic sensor correlations (speed ↔ RPM ↔ temp ↔ pedals), continuous data streaming  
# **Output**: Telemetry events sent to Azure Event Hub every 2 seconds  
# **Duration**: Typically runs for 2 hours (120 minutes) generating continuous vehicle data  
# **Progress**: Status printed every batch showing vehicle count, elapsed time, and remaining duration

# CELL ********************

%pip install faker --quiet
%pip install azure-eventhub --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import random
import time
import datetime
import uuid
import math
import os
from datetime import datetime, timedelta
import sempy.fabric as fabric
import azure.eventhub
from azure.eventhub import EventHubProducerClient, EventData
from faker import Faker

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Faker
fake = Faker()

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

eventhub_connection_str = get_eventstream_connection_string(eventstream_name = "Vehicle_Eventstream", eventstream_source_name = "VehicleTelemetrySource")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read routes from a Lakehouse
try:
    with open("/lakehouse/default/Files/data/vehicle_route_points.json", 'r') as f:
        routes = json.loads(json.load(f))
    
    print('Successfully retrieved stored routes.')

    # Display the routes and count the number of points
    for route in routes:
        display('Route ' + route['route_id'] + ' has ' + str(len(route['points'])) + ' points.')

except Exception as e:
    print('Failed to retrieve stored routes.')
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_vehicle_ids():
    """Generate IDs for a vehicle telemetry event"""
    return {
        'EventID': str(uuid.uuid4()),
        'JourneyID': f"journey_{fake.uuid4()[:8]}",
        'DriverID': f"driver_{fake.uuid4()[:8]}",
        'EventCategoryID': random.randint(1, 5)
    }

def calculate_rpm_from_speed_and_gear(speed, gear):
    """Calculate engine RPM based on speed and gear"""
    if gear == 0:  # neutral
        return random.uniform(700, 900)  # idle RPM
    elif gear == -1:  # reverse
        return 1000 + (speed * 80)  # reverse has higher RPM for same speed
    else:  # forward gears
        # Different gear ratios affect RPM at a given speed
        gear_factors = {1: 120, 2: 70, 3: 45, 4: 30, 5: 24, 6: 20}
        rpm = 800 + (speed * gear_factors.get(gear, 25))
        return min(5000, rpm)  # Cap at 5000 RPM

def determine_gear_from_speed(speed, prev_gear=None, accelerating=True):
    """Determine appropriate gear based on vehicle speed and direction of acceleration"""
    if speed < 1:
        return 0  # neutral/park when stopped
    
    # If we were in reverse and still going slow, stay in reverse
    if prev_gear == -1 and speed < 10:
        return -1
    
    # Forward gears based on speed ranges
    # These ranges overlap to prevent constant gear changes
    if accelerating:
        if speed < 15: return 1
        elif speed < 30: return 2
        elif speed < 50: return 3
        elif speed < 70: return 4
        elif speed < 90: return 5
        else: return 6
    else:  # When decelerating, hold gears longer
        if speed < 10: return 1
        elif speed < 25: return 2
        elif speed < 40: return 3
        elif speed < 60: return 4
        elif speed < 80: return 5
        else: return 6

def generate_correlated_engine_data(previous_data=None, time_delta=1.0):
    """
    Generate realistically correlated engine and vehicle performance data
    
    Parameters:
    - previous_data: The previous telemetry data point (for continuity)
    - time_delta: Time elapsed since previous reading in seconds
    """
    # Starting with defaults or previous values
    if previous_data:
        # Extract previous values to build upon
        prev_speed = previous_data.get('vehicle_speed', random.uniform(50, 100))
        prev_rpm = previous_data.get('engine_rpm', 1000)
        prev_gear = previous_data.get('transmission_gear_position', 1)
        prev_accel = previous_data.get('accelerator_pedal_position', 20)
        prev_brake = previous_data.get('brake_pedal_position', 0)
        prev_engine_temp = previous_data.get('engine_temp', 85)
        prev_trans_temp = previous_data.get('transmission_temp', 82)
        prev_torque = previous_data.get('torque_at_transmission', 1000)
        prev_steering = previous_data.get('steering_wheel_angle', 0)
    else:
        # Initialize with reasonable defaults
        prev_speed = random.uniform(50, 100)
        prev_gear = determine_gear_from_speed(prev_speed)
        prev_rpm = calculate_rpm_from_speed_and_gear(prev_speed, prev_gear)
        prev_accel = random.uniform(20, 50)
        prev_brake = 0 if prev_accel > 15 else random.uniform(10, 30)
        prev_engine_temp = random.uniform(80, 90)
        prev_trans_temp = random.uniform(75, 85)
        prev_torque = random.uniform(500, 2000)
        prev_steering = random.uniform(-20, 20)

    
    # Preferred cruising speed
    preferred_speed = 55 
    accelerator_speed_adjustment = ((preferred_speed - prev_speed)/preferred_speed)
    
    # Calculate acceleration/deceleration based on pedal positions
    # Randomly adjust pedal positions (driver behavior)
    if random.random() < 0.25:  # 20% chance to change acceleration behavior
        accel_pedal = max(10, min(80, prev_accel + random.uniform(-15, 15) + accelerator_speed_adjustment*10))
    else:
        accel_pedal = max(10, min(80, prev_accel + random.uniform(-5, 5) + accelerator_speed_adjustment*3))
    
    if random.random() < 0.10:  # 10% chance to change braking behavior
        brake_pedal = max(0, min(100, prev_brake + random.uniform(-30, 30) - accelerator_speed_adjustment*10))
    else:
        brake_pedal = max(0, min(100, prev_brake + random.uniform(-10, 10)- accelerator_speed_adjustment*3))
    

    # Ensure pedals are not both pressed heavily at the same time
    if accel_pedal > 40 and brake_pedal > 40:
        if random.random() < 0.5:  # Prioritize accelerator or brake
            accel_pedal = random.uniform(20, 40)
            brake_pedal = 0
        else:
            brake_pedal = random.uniform(0, 10)
            accel_pedal= 0

    # Calculate speed changes based on pedal positions
    acceleration = (accel_pedal) / 70 * 3  # 3 m/s² max acceleration
    deceleration = (brake_pedal / 100) * 7  # 7 m/s² max deceleration
    
    speed_change = (acceleration - deceleration) * time_delta
    
    # Apply speed change (convert m/s² to km/h change)
    speed_change_kmh = speed_change * 3.6
    speed = max(0, min(120, prev_speed + speed_change_kmh))
    
    # Determine gear based on speed and acceleration
    accelerating = speed > prev_speed
    gear = determine_gear_from_speed(speed, prev_gear, accelerating)
    
    # Calculate engine RPM based on speed and gear
    rpm = calculate_rpm_from_speed_and_gear(speed, gear)
    
    # Torque correlates with accelerator position and RPM
    torque_factor = accel_pedal / 80  # 0-1 scale based on accelerator
    rpm_factor = min(1, rpm / 3000)   # 0-1 scale based on RPM
    
    # Calculate torque (peak at mid-range RPMs)
    if rpm < 1000:
        rpm_efficiency = rpm / 1000
    elif rpm < 3500:
        rpm_efficiency = 0.9 + (rpm - 1000) / 25000
    else:
        rpm_efficiency = 1 - (rpm - 3500) / 15000
    
    max_torque = 5000
    torque = max_torque * torque_factor * rpm_efficiency
    
    # Smooth torque transitions
    torque = prev_torque * 0.7 + torque * 0.3
    torque = min(max_torque, max(0, torque))
    
    # Engine and transmission temperatures
    # Temperatures increase with load, but slowly
    temp_increase_factor = (speed / 120) * (rpm / 5000) * 0.3 * time_delta
    
    engine_temp = prev_engine_temp + temp_increase_factor
    if speed < 20:  # Cooling when idle or slow
        engine_temp -= 0.1 * time_delta
        
    trans_temp = prev_trans_temp + temp_increase_factor * 0.8
    if speed < 10:  # Transmission cools slower
        trans_temp -= 0.05 * time_delta
        
    # Keep temperatures in realistic ranges
    engine_temp = min(120, max(70, engine_temp))
    trans_temp = min(120, max(70, trans_temp))
    
    # Steering wheel angle - changes gradually
    steering_change = random.uniform(-15, 15)
    steering_wheel_angle = max(-180, min(188, prev_steering + steering_change))
    
    return {
        'vehicle_speed': round(speed, 2),
        'engine_rpm': round(rpm, 2),
        'transmission_gear_position': gear,
        'gear_lever_position': gear,  # Typically matches transmission gear
        'accelerator_pedal_position': round(accel_pedal, 2),
        'brake_pedal_position': round(brake_pedal, 2),
        'torque_at_transmission': round(torque, 2),
        'engine_temp': round(engine_temp, 2),
        'transmission_temp': round(trans_temp, 2),
        'steering_wheel_angle': round(steering_wheel_angle, 2)
    }

def generate_tire_pressure(previous_data=None):
    """
    Generate realistic tire pressure data
    If previous data exists, make minor adjustments rather than generating brand new values
    """
    if previous_data and 'tire_pressure' in previous_data:
        # Start with previous values and make small adjustments
        previous_pressures = previous_data['tire_pressure']
        tire_pressure = {}
        
        for i in range(1, 7):
            key = str(i)
            prev_value = previous_pressures.get(key, random.uniform(30, 35))
            # Small random fluctuation (±0.2 PSI)
            new_value = prev_value + random.uniform(-0.2, 0.2)
            tire_pressure[key] = round(min(70, max(25, new_value)), 2)
    else:
        # Generate new tire pressure values
        base_pressure = random.uniform(32, 36)
        variation = 2.0  # PSI variation between tires
        
        tire_pressure = {
            str(i): round(base_pressure + random.uniform(-variation, variation), 2)
            for i in range(1, 7)
        }
    
    return tire_pressure

def generate_vehicle_status(previous_data=None):
    """
    Generate vehicle status data
    If previous data exists, ensure status changes are realistic
    """
    if previous_data:
        # Most status values shouldn't change frequently
        prev_door = previous_data.get('door_status', 'all_locked')
        prev_ignition = previous_data.get('ignition_status', 1)
        prev_headlamp = previous_data.get('headlamp_status', 0)
        prev_highbeam = previous_data.get('high_beam_status', 0)
        prev_wiper = previous_data.get('windshield_wiper_status', 0)
        prev_fuel = previous_data.get('fuel_level', 50.0)
        
        # Door status changes occasionally
        if random.random() < 0.02:  # 2% chance to change
            door_options = ["all_unlocked", "all_locked", "partially_locked"]
            door_status = random.choice([d for d in door_options if d != prev_door])
        else:
            door_status = prev_door
            
        # Ignition rarely changes during continuous telemetry
        ignition_status = prev_ignition
        
        # Headlamps might change occasionally
        if random.random() < 0.05:  # 5% chance
            headlamp_status = 1 - prev_headlamp  # Toggle
        else:
            headlamp_status = prev_headlamp
            
        # High beams might change occasionally if headlamps are on
        if headlamp_status == 1 and random.random() < 0.03:
            high_beam_status = 1 - prev_highbeam
        else:
            high_beam_status = prev_highbeam
            
        # Wipers might change occasionally
        if random.random() < 0.04:
            windshield_wiper_status = 1 - prev_wiper
        else:
            windshield_wiper_status = prev_wiper
            
        # Fuel decreases slightly with usage
        speed = previous_data.get('vehicle_speed', 0)
        fuel_consumption = speed * 0.0001  # Higher speeds use more fuel
        fuel_level = max(10, prev_fuel - fuel_consumption)
        
    else:
        # Initial values
        door_options = ["all_unlocked", "all_locked", "partially_locked"]
        door_status = random.choice(door_options)
        ignition_status = 1  # Usually on when sending telemetry
        headlamp_status = random.randint(0, 1)
        high_beam_status = 0 if headlamp_status == 0 else random.randint(0, 1)
        windshield_wiper_status = random.randint(0, 1)
        fuel_level = round(random.uniform(20, 99.9995), 6)
        
    return {
        'door_status': door_status,
        'ignition_status': ignition_status,
        'headlamp_status': headlamp_status,
        'high_beam_status': high_beam_status,
        'windshield_wiper_status': windshield_wiper_status,
        'fuel_level': fuel_level,
        'parking_brake_status': ""  # Empty as per requirements
    }

def generate_telemetry_event(vehicle_id=None, journey_id=None, driver_id=None, previous_data=None, lat=None, lon=None, time_delta=1.0):
    """
    Generate a complete telemetry event
    
    Parameters:
    - vehicle_id: Vehicle ID (will be generated if None)
    - journey_id: Journey ID (will be generated if None)
    - driver_id: Driver ID (will be generated if None)
    - previous_data: Previous telemetry event (for realistic transitions)
    - lat: Latitude
    - lon: Longitude
    - time_delta: Time in seconds since previous event,
    """
    # Generate IDs if not provided
    if not journey_id or not driver_id:
        ids = generate_vehicle_ids()
        journey_id = journey_id or ids['JourneyID']
        driver_id = driver_id or ids['DriverID']
    
    event_id = str(uuid.uuid4())
    event_category_id = random.randint(1, 5)
    
    # Get engine and performance data
    engine_data = generate_correlated_engine_data(previous_data, time_delta)
    
    # Get vehicle status
    vehicle_status = generate_vehicle_status(previous_data)
    
    # Get tire pressure
    tire_pressure = generate_tire_pressure(previous_data)
    
    # Generate or update odometer reading
    if previous_data and 'odometer' in previous_data:
        # Increment odometer based on speed (km traveled in time_delta seconds)
        distance = engine_data['vehicle_speed'] * time_delta / 3600  # km traveled
        odometer = round(previous_data['odometer'] + distance, 2)
    else:
        odometer = round(random.uniform(0, 250000.00), 2)
    
    # Timestamp
    timestamp = datetime.utcnow().isoformat()
    
    # Combine all data
    telemetry_event = {
        'EventID': event_id,
        'JourneyID': journey_id,
        'VehicleID': vehicle_id,
        'DriverID': driver_id,
        'EventCategoryID': event_category_id,
        'timestamp': timestamp,
        'odometer': odometer,
        'tire_pressure': tire_pressure,
        'lat': lat,
        'lon': lon,
        **engine_data,
        **vehicle_status
    }
    
    return telemetry_event

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def send_telemetry_to_eventhub(connection_str, routes, duration_minutes: int = 120, verbose=True):
    """
    Generate and send vehicle telemetry data to Azure Event Hub
    
    Parameters:
    - connection_str: Azure Event Hub connection string
    - verbose: Whether to print status messages
    """

    # Create a producer client
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_str
    )
    
    # Generate data for multiple vehicles
    vehicles = routes

    # Create vehicles
    for vehicle in vehicles:
        ids = generate_vehicle_ids()
        vehicle['vehicle_id'] = 'Truck' + vehicle['route_id']
        vehicle['journey_id'] = ids['JourneyID']
        vehicle['driver_id'] = ids['DriverID'],
        vehicle['previous_data'] =  None
    
      
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=duration_minutes)
    
    current_time = start_time
   
    try:
        while current_time < end_time:
            # Create a batch
            event_data_batch = producer.create_batch()
            
            # Generate telemetry for each vehicle
            for vehicle in vehicles:
                # Move to next point (at the end of the route, restart at beginning)
                current_point = vehicle['current_point'] + 1 if vehicle['current_point'] < vehicle['total_points']-1 else 0
                vehicle['current_point'] = current_point

                # Generate a single telemetry event for this vehicle
                telemetry = generate_telemetry_event(
                    vehicle_id=vehicle['vehicle_id'],
                    journey_id=vehicle['journey_id'],
                    driver_id=vehicle['driver_id'],
                    previous_data=vehicle['previous_data'],
                    lat = vehicle['points'][current_point][0],
                    lon = vehicle['points'][current_point][1],
                    time_delta=1.0  # Assuming 1 seconds between readings
                )
                
                # Update previous data for next iteration
                vehicle['previous_data'] = telemetry
                
                # Add to batch
                event_data_batch.add(EventData(json.dumps(telemetry)))

            # Send the batch to Event Hub
            producer.send_batch(event_data_batch)
            if verbose:
                print(f"Sent {len(vehicles)} telemetry events to Event Hub")
            
            # Wait before sending next batch
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("Telemetry generation stopped")
    finally:
        producer.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run the simulation for 2 hours
print("🚀 Starting vehicle telemetry simulation...")

try:
    send_telemetry_to_eventhub(eventhub_connection_str, routes, duration_minutes=120, verbose=False)
except KeyboardInterrupt:
    print("\n⚠️  Stopped by user")
except Exception as e:
    print(f"\n❌ Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
