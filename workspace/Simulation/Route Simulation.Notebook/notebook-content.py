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

# # Vehicle Route Generator
# 
# Generates realistic vehicle routes using Azure Maps APIs to find points of interest and calculate road network paths.
# 
# **Features**: POI discovery via Azure Maps, realistic route calculation following road networks, route segmentation for smooth telemetry playback  
# **Output**: JSON file with route coordinates saved to Lakehouse Files  
# **Duration**: Quick (~1-2 minutes depending on API calls and route count)  
# **Requirements**: Azure Maps subscription key stored in Azure Key Vault
# 
# <mark>Note: this notebook requires a subscription to the Azure Maps and relies on Azure Key Vault to securely store and retrieve API Keys. However, this notebook is optional -- sample vehicle route data has been pre-loaded into the files area of the Lakehouse and will be used even if you do not execute this notebook.</mark>


# CELL ********************

%pip install shapely --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json
import shapely
import random
import json
import math
import sys

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve Azure Maps subscription key from Key Vault

AzureKeyVaultName = 'https://<yourname>.vault.azure.net/'
AzureKeyVaultSecretName = 'MySecretName'

try:
    AzureMapsKey = notebookutils.credentials.getSecret(AzureKeyVaultName, AzureKeyVaultSecretName)
    print("✅ Azure Maps key retrieved from Key Vault")
except Exception as e:
    print(f"❌ Error retrieving Azure Maps key.")
    print("   In order to continue, create an Azure Maps account, store API Key in the Key Vault and update AzureKeyVaultName and AzureKeyVaultSecretName variables.")
    print("✅  Keep in mind, this notebook is optional -- sample vehicle route data has been pre-loaded into the files area of the Lakehouse and will be used even if you do not execute this notebook.")
    notebookutils.session.stop()

# Define POI search parameters
query = 'Restaurant'  # Type of points of interest to visit
radius = 5000  # Search radius in meters

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_POIs(latitude, longitude, query, radius, subscription_key):
    """Get points of interest from Azure Maps with error handling"""
    
    # Azure Maps Search POI Category API URL
    url = 'https://atlas.microsoft.com/search/poi/category/json'

    # Parameters for the API call
    params = {
        'api-version': 1.0,
        'subscription-key': subscription_key,
        'query': query,
        'lat': latitude,
        'lon': longitude,
        'radius': radius,
        'limit': 100
    }

    try:
        # Make the API call with timeout
        response = requests.get(url, params=params, timeout=30)

        # Check if the request was successful
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Azure Maps POI API error: {response.status_code} - {response.text[:200]}")
            return None
            
    except requests.Timeout:
        print("❌ Azure Maps API request timed out")
        return None
    except Exception as e:
        print(f"❌ Error calling Azure Maps POI API: {str(e)[:150]}")
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_waypoints(start, POIs, end):
    # Start with starting point
    waypoints = str(start[0]) + "," + str(start[1]) + ":"
    
    for result in POIs['results']:
        waypoint = str(result['position']['lat']) + "," + str(result['position']['lon']) + ":"
        waypoints += waypoint
    
    # End with ending point
    waypoints += str(start[0]) + "," + str(start[1]) + ":"
    # Remove the trailing colon
    waypoints = waypoints.rstrip(":")
    
    return waypoints

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_route(waypoints, subscription_key):

    # Azure Maps Route Directions API URL
    url = 'https://atlas.microsoft.com/route/directions/json'

    # Parameters for the API call
    params = {
        'api-version': 1.0,
        'subscription-key': subscription_key,
        'query': waypoints,
        'report': 'effectiveSettings',
        'travelMode': 'truck',
        'computeBestOrder': 'true',
        'routeType':'shortest'
    }

    # Headers
    headers = {
        'Content-Type': 'application/json',
        'subscription-key': subscription_key
    }

    # Make the API call
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response JSON
        return response.json()
    else:
        print(f"Failed to retrieve route directions: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

def get_route_segments(route):
    points = []
    
    for route in route["routes"]:
        for leg in route["legs"]:
            for point in leg["points"]:
                #segment = "(" + str(point['latitude']) + "," + str(point['longitude']) + ")"
                coords = [point['latitude'], point['longitude']]
                points.append(coords)
    
    route_line_string = shapely.LineString(points)
    route_segments = shapely.segmentize(route_line_string, max_segment_length=.0002)
    route_segment_coordinates = shapely.get_coordinates(route_segments)
    return route_segment_coordinates.tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_route_points(coordinates, AzureMapsKey):
    latitude = float(coordinates[0])
    longitude = float(coordinates[1])

    POIs = get_POIs(latitude, longitude, query, radius, AzureMapsKey)
    waypoints = get_waypoints([latitude, longitude], POIs, [latitude, longitude])
    route = get_route(waypoints, AzureMapsKey)
    route_points = get_route_segments(route)
    return route_points

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Initialize the routes that will serve as the starting points for route planning
routes = [
    {"route_id": "46201", "centroid_coordinates": (39.7805, -86.1105)},
    {"route_id": "46202", "centroid_coordinates": (39.7823, -86.1573)},
    {"route_id": "46203", "centroid_coordinates": (39.7426, -86.1256)},
    {"route_id": "46204", "centroid_coordinates": (39.7726, -86.1551)},
    {"route_id": "46205", "centroid_coordinates": (39.8202, -86.1382)},
    {"route_id": "46208", "centroid_coordinates": (39.8161, -86.1675)},
    {"route_id": "46214", "centroid_coordinates": (39.7934, -86.2914)},
    {"route_id": "46217", "centroid_coordinates": (39.6594, -86.1806)},
    {"route_id": "46218", "centroid_coordinates": (39.8136, -86.1022)},
    {"route_id": "46219", "centroid_coordinates": (39.7879, -86.0277)},
    {"route_id": "46220", "centroid_coordinates": (39.8881, -86.1236)},
    {"route_id": "46222", "centroid_coordinates": (39.7913, -86.2102)}
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get route points
for route in routes:
    route["points"] = get_route_points(route['centroid_coordinates'], AzureMapsKey)
    route["total_points"] = len(route["points"])
    route['current_point'] = random.randint(0, route["total_points"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os

#Save routes to a Lakehouse file

# Target path in Lakehouse Files
output_path = "/lakehouse/default/Files/data/vehicle_route_points.json"

# Ensure directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Write JSON to file (pretty-printed)
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(routes, f, indent=2, ensure_ascii=False)

print(f"✅ JSON written to: {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display the routes and count the number of points
for route in routes:
    display('Route ' + route['route_id'] + ' has ' + str(len(route['points'])) + ' points and is shaped as follows:')
    display(shapely.LineString(route["points"]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
