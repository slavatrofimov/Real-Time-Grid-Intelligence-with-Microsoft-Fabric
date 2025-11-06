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

# # Thunderstorm System Simulation
# 
# Generates realistic GeoJSON polygons representing a severe thunderstorm system with nested intensity zones (light → moderate → heavy → extra heavy rain) that move across the city and dissipate.
# 
# **Features**: Irregular storm shapes with smooth curves, nested intensity polygons with heavy rain at leading edge, realistic movement and growth/dissipation patterns  
# **Output**: GeoJSON FeatureCollections streamed to Azure Event Hub every 2 seconds  
# **Duration**: 140 seconds total (120s progression + 20s dissipation = 70 frames)  
# **Progress**: Status printed for every frame with percentage, phase, intensity, and position

# MARKDOWN ********************

# ## 1. Setup

# CELL ********************

%pip install shapely geojson azure-eventhub matplotlib numpy --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time
import math
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import sempy.fabric as fabric

# Geospatial libraries
import geojson
from shapely.geometry import Polygon, Point
from shapely.ops import unary_union
import numpy as np

# Visualization (optional, for debugging)
import matplotlib.pyplot as plt

# Azure Event Hub
from azure.eventhub import EventHubProducerClient, EventData

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. City Geographic Boundaries

# CELL ********************

# City center
CITY_CENTER = {
    'lat': 39.76838,
    'lon': -86.15804
}

CITY_BOUNDS_NORTH = CITY_CENTER['lat'] + .15
CITY_BOUNDS_SOUTH = CITY_CENTER['lat'] - 0.25
CITY_BOUNDS_EAST = CITY_CENTER['lon'] + .25
CITY_BOUNDS_WEST = CITY_CENTER['lon'] - 0.3

# City boundaries (approximate)
CITY_BOUNDS = {
    'north': CITY_BOUNDS_NORTH,   # Northern boundary
    'south': CITY_BOUNDS_SOUTH,   # Southern boundary  
    'east': CITY_BOUNDS_EAST,   # Eastern boundary
    'west': CITY_BOUNDS_WEST    # Western boundary
}

# Calculate city dimensions
CITY_WIDTH = CITY_BOUNDS['east'] - CITY_BOUNDS['west']  
CITY_HEIGHT = CITY_BOUNDS['north'] - CITY_BOUNDS['south']  

print(f"City boundaries defined:")
print(f"  North: {CITY_BOUNDS['north']}")
print(f"  South: {CITY_BOUNDS['south']}")
print(f"  East: {CITY_BOUNDS['east']}")
print(f"  West: {CITY_BOUNDS['west']}")
print(f"  City dimensions: {CITY_WIDTH:.3f}° x {CITY_HEIGHT:.3f}°")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Storm System Parameters

# CELL ********************

# Storm system configuration
STORM_CONFIG = {
    # Timing parameters
    'total_duration_seconds': 140,  
    'progression_duration_seconds': 120,  
    'dissipation_duration_seconds': 20,   
    'frame_interval_seconds': 2,         
    
    # Storm size parameters (in degrees)
    'initial_storm_width': 0.005,    # Initial storm width
    'initial_storm_height': 0.01,   # Initial storm height
    'max_storm_width': 0.16,        # Maximum storm width
    'max_storm_height': 0.26,       # Maximum storm height
    
    # Storm movement parameters
    'movement_direction': 'east',    # Primary direction (west to east)
    'movement_speed_deg_per_sec': CITY_WIDTH / 60,  # cross city during progression duration
    'wind_variation_degrees': 50,    # Wind direction variation
    
    # Rain intensity zones (nested polygons)
    'intensity_zones': {
        'extra_heavy': {'color': '#FF0000', 'size_factor': 0.15, 'name': 'Extra Heavy Rain'},
        'heavy': {'color': '#FF8C00', 'size_factor': 0.3, 'name': 'Heavy Rain'},
        'moderate': {'color': '#FFD700', 'size_factor': 0.6, 'name': 'Moderate Rain'},
        'light': {'color': '#32CD32', 'size_factor': 1.0, 'name': 'Light Rain'}
    }
}

# Calculate total number of frames
TOTAL_FRAMES = STORM_CONFIG['total_duration_seconds'] // STORM_CONFIG['frame_interval_seconds']
PROGRESSION_FRAMES = STORM_CONFIG['progression_duration_seconds'] // STORM_CONFIG['frame_interval_seconds']
DISSIPATION_FRAMES = STORM_CONFIG['dissipation_duration_seconds'] // STORM_CONFIG['frame_interval_seconds']

# Storm starting position (west of City)
STORM_START_POSITION = {
    'lat': CITY_CENTER['lat'] + random.uniform(-0.15, 0.15),  # Slight north/south variation
    'lon': CITY_BOUNDS['west'] - 0.35  # Start west of the city
}  # Start west of the city

print(f"Storm system parameters:")
print(f"  Total frames: {TOTAL_FRAMES}")
print(f"  Progression frames: {PROGRESSION_FRAMES}")
print(f"  Dissipation frames: {DISSIPATION_FRAMES}")
print(f"  Movement speed: {STORM_CONFIG['movement_speed_deg_per_sec']:.6f} deg/sec")
print(f"  Starting position: {STORM_START_POSITION['lat']:.4f}, {STORM_START_POSITION['lon']:.4f}")
print(f"  Intensity zones: {list(STORM_CONFIG['intensity_zones'].keys())}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Storm Shape Generation
# 
# Irregular polygon shapes with smooth curves to resemble natural storm cells.

# CELL ********************

class StormShapeGenerator:
    """Generate realistic irregular storm polygon shapes"""
    
    @staticmethod
    def generate_irregular_polygon(center_lat: float, center_lon: float, 
                                 width: float, height: float, 
                                 num_points: int = 16, 
                                 irregularity: float = 0.3) -> Polygon:
        """
        Generate an irregular polygon that resembles a natural storm cell with smooth curves
        
        Args:
            center_lat, center_lon: Center coordinates
            width, height: Approximate dimensions
            num_points: Number of vertices (kept moderate for smooth curves)
            irregularity: How irregular the shape is (0-1)
        """
        # Generate base points with smooth variation
        base_points = []
        angle_step = 2 * math.pi / num_points
        
        # Create smooth radius variations using sine waves
        radius_variations = []
        for i in range(num_points):
            # Use multiple sine waves for natural-looking variation
            variation = (
                0.7 * math.sin(i * 2 * math.pi / num_points * 3) +  # Primary variation
                0.2 * math.sin(i * 2 * math.pi / num_points * 7) +  # Secondary variation
                0.1 * math.sin(i * 2 * math.pi / num_points * 11)   # Fine detail
            )
            radius_variations.append(variation * irregularity)
        
        for i in range(num_points):
            # Base angle for this point
            angle = i * angle_step
            
            # Calculate base radius (elliptical)
            radius_x = width / 2
            radius_y = height / 2
            
            # Elliptical radius calculation
            radius = (radius_x * radius_y) / math.sqrt(
                (radius_y * math.cos(angle))**2 + (radius_x * math.sin(angle))**2
            )
            
            # Apply smooth radius variation
            radius *= (1 + radius_variations[i])
            
            # Convert to lat/lon coordinates
            lat_offset = radius * math.sin(angle)
            lon_offset = radius * math.cos(angle)
            
            lat = center_lat + lat_offset
            lon = center_lon + lon_offset
            
            base_points.append((lon, lat))  # GeoJSON uses lon, lat order
        
        # Create smooth interpolated points for even smoother curves
        smooth_points = []
        interpolation_factor = 2  # Add 1 interpolated point between each base point
        
        for i in range(len(base_points)):
            # Add the current point
            smooth_points.append(base_points[i])
            
            # Add interpolated point(s) to next point
            next_i = (i + 1) % len(base_points)
            current_point = base_points[i]
            next_point = base_points[next_i]
            
            for j in range(1, interpolation_factor + 1):
                t = j / (interpolation_factor + 1)
                # Smooth interpolation
                interp_lon = current_point[0] + t * (next_point[0] - current_point[0])
                interp_lat = current_point[1] + t * (next_point[1] - current_point[1])
                smooth_points.append((interp_lon, interp_lat))
        
        return Polygon(smooth_points)
    
    @staticmethod
    def generate_specialized_storm_cell(center_lat: float, center_lon: float, 
                                      width: float, height: float,
                                      irregularity: float, num_points: int,
                                      variation_pattern: str) -> Polygon:
        """
        Generate a storm cell with specialized characteristics for different intensity zones
        
        Args:
            center_lat, center_lon: Center coordinates
            width, height: Dimensions
            irregularity: Irregularity factor
            num_points: Number of vertices
            variation_pattern: Type of variation ('smooth', 'medium', 'active', 'intense')
        """
        # Generate base points with pattern-specific variation
        base_points = []
        angle_step = 2 * math.pi / num_points
        
        # Create pattern-specific radius variations
        radius_variations = []
        for i in range(num_points):
            if variation_pattern == 'smooth':
                # Gentle, flowing variations - light rain
                variation = (
                    0.8 * math.sin(i * 2 * math.pi / num_points * 4) +
                    0.2 * math.sin(i * 2 * math.pi / num_points * 7)
                )
            elif variation_pattern == 'medium':
                # Moderate variations with some detail - moderate rain
                variation = (
                    0.6 * math.sin(i * 2 * math.pi / num_points * 3) +
                    0.3 * math.sin(i * 2 * math.pi / num_points * 7) +
                    0.1 * math.sin(i * 2 * math.pi / num_points * 13)
                )
            elif variation_pattern == 'active':
                # More complex patterns - heavy rain
                variation = (
                    0.5 * math.sin(i * 2 * math.pi / num_points * 4) +
                    0.3 * math.sin(i * 2 * math.pi / num_points * 8) +
                    0.1 * math.sin(i * 2 * math.pi / num_points * 10)
                )
            else:  # 'intense'
                # Very complex, turbulent patterns - extra heavy rain
                variation = (
                    0.3 * math.sin(i * 2 * math.pi / num_points * 3) +
                    0.2 * math.sin(i * 2 * math.pi / num_points * 8) +
                    0.1 * math.sin(i * 2 * math.pi / num_points * 10) +
                    0.05 * math.sin(i * 2 * math.pi / num_points * 12)
                )
            
            radius_variations.append(variation * irregularity)
        
        for i in range(num_points):
            # Base angle for this point
            angle = i * angle_step
            
            # Calculate base radius (elliptical)
            radius_x = width / 2
            radius_y = height / 2
            
            # Elliptical radius calculation
            radius = (radius_x * radius_y) / math.sqrt(
                (radius_y * math.cos(angle))**2 + (radius_x * math.sin(angle))**2
            )
            
            # Apply pattern-specific radius variation
            radius *= (1 + radius_variations[i])
            
            # Convert to lat/lon coordinates
            lat_offset = radius * math.sin(angle)
            lon_offset = radius * math.cos(angle)
            
            lat = center_lat + lat_offset
            lon = center_lon + lon_offset
            
            base_points.append((lon, lat))  # GeoJSON uses lon, lat order
        
        # Create smooth interpolated points, with different interpolation based on pattern
        smooth_points = []
        if variation_pattern in ['smooth', 'medium']:
            interpolation_factor = 3  # More interpolation for smoother zones
        else:
            interpolation_factor = 2  # Less interpolation for more angular intense zones
        
        for i in range(len(base_points)):
            # Add the current point
            smooth_points.append(base_points[i])
            
            # Add interpolated point(s) to next point
            if interpolation_factor > 0:
                next_i = (i + 1) % len(base_points)
                current_point = base_points[i]
                next_point = base_points[next_i]
                
                for j in range(1, interpolation_factor + 1):
                    t = j / (interpolation_factor + 1)
                    # Smooth interpolation
                    interp_lon = current_point[0] + t * (next_point[0] - current_point[0])
                    interp_lat = current_point[1] + t * (next_point[1] - current_point[1])
                    smooth_points.append((interp_lon, interp_lat))
        
        return Polygon(smooth_points)
    
    @staticmethod
    def create_nested_intensity_zones(center_lat: float, center_lon: float,
                                    base_width: float, base_height: float,
                                    intensity_factor: float = 1.0) -> Dict[str, Polygon]:
        """
        Create nested polygons for different rain intensity zones with heavy rain at the front
        Each intensity zone has distinct visual characteristics
        
        Returns dict with intensity level as key and polygon as value
        """
        zones = {}
        
        # Define the offset multipliers for each intensity zone
        # Higher intensity zones are positioned more toward the front (east) of the storm
        zone_offsets = {
            'light': 0.0,        # Light rain stays at storm center
            'moderate': 0.1,    # Moderate rain slightly forward
            'heavy': 0.2,       # Heavy rain toward the front
            'extra_heavy': 0.25  # Extra heavy rain at the leading edge
        }
        
        # Define unique characteristics for each intensity zone
        zone_characteristics = {
            'light': {
                'shape_factor': 0.95,     # More circular/elliptical
                'irregularity_boost': 0.07,  # Smooth edges
                'aspect_ratio': 1.1,     # Slightly elongated
                'num_points_base': 37,   # Moderate detail
                'variation_pattern': 'smooth'  # Gentle variations
            },
            'moderate': {
                'shape_factor': 0.9,     # Slightly more compact
                'irregularity_boost': 0.1,  # A bit more irregular
                'aspect_ratio': 1.2,     # More elongated
                'num_points_base': 45,   # More detail
                'variation_pattern': 'medium'  # Medium variations
            },
            'heavy': {
                'shape_factor': 0.85,     # More compact and intense
                'irregularity_boost': 0.12,  # More irregular
                'aspect_ratio': 1.3,     # Quite elongated
                'num_points_base': 50,   # Higher detail
                'variation_pattern': 'active'  # More active variations
            },
            'extra_heavy': {
                'shape_factor': 0.8,     # Very compact core
                'irregularity_boost': 0.15,  # Most irregular
                'aspect_ratio': 1.4,     # Very elongated
                'num_points_base': 50,   # Highest detail
                'variation_pattern': 'intense'  # Most intense variations
            }
        }
        
        for intensity, config in STORM_CONFIG['intensity_zones'].items():
            zone_width = base_width * config['size_factor']
            zone_height = base_height * config['size_factor']
            
            # Get unique characteristics for this zone
            char = zone_characteristics[intensity]
            
            # Apply shape factor to make zones more distinct
            zone_width *= char['shape_factor']
            zone_height *= char['shape_factor'] * char['aspect_ratio']
            
            # Calculate the offset position for this intensity zone
            # Positive offset moves the zone eastward (direction of storm movement)
            offset_distance = zone_offsets[intensity] * base_width
            zone_center_lat = center_lat
            zone_center_lon = center_lon + offset_distance
            
            # Calculate unique irregularity for this zone
            base_irregularity = intensity_factor * (1.1 - config['size_factor'])
            zone_irregularity = base_irregularity + char['irregularity_boost']
            
            # Calculate number of points based on intensity characteristics
            num_points = char['num_points_base'] + int(intensity_factor * 4)
            
            # Generate the polygon with unique characteristics
            polygon = StormShapeGenerator.generate_specialized_storm_cell(
                zone_center_lat, zone_center_lon, zone_width, zone_height, 
                zone_irregularity, num_points, char['variation_pattern']
            )
            
            zones[intensity] = polygon
        
        return zones

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Storm Movement and Evolution

# CELL ********************

class StormMovement:
    """Handle storm movement and evolution over time"""
    
    def __init__(self, start_lat: float, start_lon: float):
        self.start_lat = start_lat
        self.start_lon = start_lon
        self.current_lat = start_lat
        self.current_lon = start_lon
        
    def calculate_position(self, frame_number: int, total_frames: int) -> Tuple[float, float]:
        """
        Calculate storm position for a given frame
        
        Args:
            frame_number: Current frame (0-based)
            total_frames: Total number of frames in progression
        """
        if frame_number >= PROGRESSION_FRAMES:
            # During dissipation, storm continues moving but slower
            progression_progress = 1.0
            dissipation_frame = frame_number - PROGRESSION_FRAMES
            dissipation_progress = dissipation_frame / DISSIPATION_FRAMES
            additional_movement = dissipation_progress * 0.3  # Move 30% more during dissipation
            progress = progression_progress + additional_movement
        else:
            # During progression phase
            progress = frame_number / PROGRESSION_FRAMES
        
        # Calculate distance traveled
        total_distance = CITY_WIDTH + 0.3  # Cross city plus some extra
        distance_traveled = progress * total_distance
        
        # Add some wind variation (sinusoidal pattern)
        wind_variation = math.sin(progress * math.pi * 2) * 0.02  # Small north-south drift
        
        # Calculate new position
        self.current_lat = self.start_lat + wind_variation
        self.current_lon = self.start_lon + distance_traveled
        
        return self.current_lat, self.current_lon
    
    def calculate_storm_size(self, frame_number: int) -> Tuple[float, float]:
        """
        Calculate storm size evolution over time
        
        Returns: (width, height)
        """
        if frame_number < PROGRESSION_FRAMES:
            # Growth phase - storm grows as it approaches and peaks over city
            progress = frame_number / PROGRESSION_FRAMES
            
            # Storm grows more rapidly in the middle portion
            size_multiplier = 0.7 + 0.6 * math.sin(progress * math.pi)
            
            width = STORM_CONFIG['initial_storm_width'] + \
                   (STORM_CONFIG['max_storm_width'] - STORM_CONFIG['initial_storm_width']) * size_multiplier
            height = STORM_CONFIG['initial_storm_height'] + \
                    (STORM_CONFIG['max_storm_height'] - STORM_CONFIG['initial_storm_height']) * size_multiplier
        else:
            # Dissipation phase - storm shrinks
            dissipation_frame = frame_number - PROGRESSION_FRAMES
            dissipation_progress = dissipation_frame / DISSIPATION_FRAMES
            
            # Exponential decay for realistic dissipation
            decay_factor = math.exp(-3 * dissipation_progress)
            
            width = STORM_CONFIG['max_storm_width'] * decay_factor
            height = STORM_CONFIG['max_storm_height'] * decay_factor
        
        return width, height
    
    def calculate_intensity(self, frame_number: int) -> float:
        """
        Calculate storm intensity over time (0-1 scale)
        """
        if frame_number < PROGRESSION_FRAMES:
            # Build up to peak intensity
            progress = frame_number / PROGRESSION_FRAMES
            
            # Peak intensity when storm is over the city (around frame 9-12)
            peak_frame_ratio = 0.6  # Peak at 60% through progression
            
            if progress < peak_frame_ratio:
                # Building up
                intensity = 0.3 + 0.7 * (progress / peak_frame_ratio)
            else:
                # Slight decline after peak
                decline_progress = (progress - peak_frame_ratio) / (1 - peak_frame_ratio)
                intensity = 1.0 - 0.1 * decline_progress
                
            return min(1.0, intensity)
        else:
            # Dissipation phase
            dissipation_frame = frame_number - PROGRESSION_FRAMES
            dissipation_progress = dissipation_frame / DISSIPATION_FRAMES
            
            # Exponential intensity decay
            return 0.9 * math.exp(-2 * dissipation_progress)

# Initialize storm movement
storm_movement = StormMovement(STORM_START_POSITION['lat'], STORM_START_POSITION['lon'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. GeoJSON Generation

# CELL ********************

class StormGeoJSONGenerator:
    """Generate GeoJSON data for storm visualization"""
    
    @staticmethod
    def polygon_to_geojson(polygon: Polygon, properties: Dict) -> Dict:
        """Convert Shapely polygon to GeoJSON feature"""
        # Extract coordinates (GeoJSON expects [[[lon, lat]]] format for polygons)
        coords = [list(polygon.exterior.coords)]
        
        return {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": coords
            },
            "properties": properties
        }
    
    @staticmethod
    def generate_storm_frame(frame_number: int, timestamp: datetime) -> Dict:
        """
        Generate a complete storm frame with all intensity zones
        
        Args:
            frame_number: Current frame number (0-based)
            timestamp: Timestamp for this frame
            
        Returns:
            Complete GeoJSON FeatureCollection for this frame
        """
        # Calculate storm position, size, and intensity
        lat, lon = storm_movement.calculate_position(frame_number, TOTAL_FRAMES)
        width, height = storm_movement.calculate_storm_size(frame_number)
        intensity = storm_movement.calculate_intensity(frame_number)
        
        # Generate intensity zone polygons
        storm_zones = StormShapeGenerator.create_nested_intensity_zones(
            lat, lon, width, height, intensity
        )
        
        # Create GeoJSON features for each zone
        features = []
        
        for zone_name, polygon in storm_zones.items():
            zone_config = STORM_CONFIG['intensity_zones'][zone_name]
            
            # Calculate rainfall rate based on intensity and zone
            base_rainfall_rates = {
                'light': 2.5,      # mm/hr
                'moderate': 7.5,   # mm/hr  
                'heavy': 25.0,     # mm/hr
                'extra_heavy': 50.0 # mm/hr
            }
            
            rainfall_rate = base_rainfall_rates[zone_name] * intensity
            
            properties = {
                "intensity_level": zone_name,
                "color": zone_config['color'],
                "name": zone_config['name'],
                "rainfall_rate_mm_per_hour": round(rainfall_rate, 1),
                "storm_intensity": round(intensity, 3),
                "frame_number": frame_number,
                "timestamp": timestamp.isoformat(),
                "center_lat": round(lat, 6),
                "center_lon": round(lon, 6),
                "storm_width_degrees": round(width, 6),
                "storm_height_degrees": round(height, 6)
            }
            
            feature = StormGeoJSONGenerator.polygon_to_geojson(polygon, properties)
            features.append(feature)
        
        # Create complete GeoJSON FeatureCollection
        geojson_data = {
            "type": "FeatureCollection",
            "properties": {
                "storm_system_id": f"detroit_storm_{timestamp.strftime('%Y%m%d_%H%M%S')}",
                "frame_number": frame_number,
                "total_frames": TOTAL_FRAMES,
                "timestamp": timestamp.isoformat(),
                "location": "City, MI",
                "storm_phase": "progression" if frame_number < PROGRESSION_FRAMES else "dissipation",
                "storm_center": {"lat": lat, "lon": lon},
                "overall_intensity": round(intensity, 3)
            },
            "features": features
        }
        
        return geojson_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Azure Event Hub Integration

# CELL ********************

# Get connection string for a given Eventstream
def get_eventstream_connection_string(eventstream_name, eventstream_source_name):
    workspace_id = fabric.resolve_workspace_id()
    
    #Get Eventstream Id
    eventstream_id = eventhouse_id = fabric.resolve_item_id(eventstream_name)
    
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retrieve Event Hub connection string
try:
    eventstream_connection_string = get_eventstream_connection_string(
        eventstream_name="Weather_EventStream", 
        eventstream_source_name="WeatherSource"
    )
    print("✅ Event Hub connection configured")
except Exception as e:
    print(f"⚠️  Warning: Could not retrieve Event Hub connection: {e}")
    eventstream_connection_string = None

# Event Hub Configuration
EVENT_HUB_CONFIG = {
    # Replace with your actual Event Hub connection string
    'connection_string': eventstream_connection_string
}

class EventHubStormSender:
    """Handle sending storm data to Azure Event Hub"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.client = None

        try:
            self.client = EventHubProducerClient.from_connection_string(
                connection_string
            )
        except Exception as e:
            print(f"Failed to initialize Event Hub client: {e}")
            self.client = None

    
    def send_storm_event(self, geojson_data: Dict, frame_number: int) -> bool:
        """
        Send storm GeoJSON data to Event Hub
        
        Args:
            geojson_data: The GeoJSON storm data
            frame_number: Current frame number
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert GeoJSON to JSON string
            json_message = json.dumps(geojson_data, separators=(',', ':'))
            
            if self.client:
                # Send to actual Event Hub
                event_data = EventData(json_message)
                
                # Add custom properties for routing/filtering
                event_data.properties = {
                    'event_type': 'storm_frame',
                    'frame_number': frame_number,
                    'location': 'detroit',
                    'data_format': 'geojson'
                }
                
                with self.client:
                    event_data_batch = self.client.create_batch()
                    event_data_batch.add(event_data)
                    self.client.send_batch(event_data_batch)
                
                print(f"✓ Sent frame {frame_number} to Event Hub ({len(json_message)} bytes)")
                return True
            else:
                # Simulate sending (for testing without actual Event Hub)
                print(f"✓ [SIMULATED] Sent frame {frame_number} ({len(json_message)} bytes)")
                return True
                
        except Exception as e:
            print(f"✗ Failed to send frame {frame_number}: {e}")
            return False
    
    def close(self):
        """Close the Event Hub connection"""
        if self.client:
            self.client.close()
            print("Event Hub client closed")

# Initialize Event Hub sender
event_hub_sender = EventHubStormSender(
    EVENT_HUB_CONFIG['connection_string']
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Run Storm Simulation
# 
# Execute the complete storm simulation with frame-by-frame progress tracking.

# CELL ********************

from typing import List, Dict

def run_storm_simulation(send_to_event_hub: bool = True, 
                        accelerated: bool = False) -> List[Dict]:
    """
    Run the complete storm simulation with enhanced progress tracking
    
    Args:
        send_to_event_hub: Whether to send data to Event Hub
        accelerated: If True, run faster for testing (1 second intervals)
        
    Returns:
        List of all generated GeoJSON frames
    """
    print("\n" + "="*70)
    print("🌩️  THUNDERSTORM SIMULATION - STARTING")
    print("="*70)
    print(f"Duration:         {STORM_CONFIG['total_duration_seconds']}s ({STORM_CONFIG['total_duration_seconds']/60:.1f} min)")
    print(f"Total frames:     {TOTAL_FRAMES}")
    print(f"Frame interval:   {STORM_CONFIG['frame_interval_seconds']}s")
    print(f"Event Hub:        {'ENABLED' if send_to_event_hub else 'DISABLED'}")
    print(f"Mode:             {'ACCELERATED (1s/frame)' if accelerated else 'REAL-TIME'}")
    print("="*70 + "\n")
    
    start_time = datetime.now()
    generated_frames = []
    successful_sends = 0
    
    for frame_number in range(TOTAL_FRAMES):
        frame_start_time = time.time()
        
        # Calculate frame timestamp
        frame_timestamp = start_time + timedelta(
            seconds=frame_number * STORM_CONFIG['frame_interval_seconds']
        )
        
        try:
            # Generate storm frame
            geojson_frame = StormGeoJSONGenerator.generate_storm_frame(
                frame_number, frame_timestamp
            )
            
            generated_frames.append(geojson_frame)
            
            # Extract frame info
            storm_phase = geojson_frame['properties']['storm_phase']
            intensity = geojson_frame['properties']['overall_intensity']
            center = geojson_frame['properties']['storm_center']
            progress_percent = ((frame_number + 1) / TOTAL_FRAMES) * 100
            
            # Determine phase icon
            phase_icon = "🌧️" if storm_phase == "progression" else "⛅"
            intensity_bar = "█" * int(intensity * 10)
            
            print(f"{phase_icon} Frame {frame_number+1:2d}/{TOTAL_FRAMES} [{progress_percent:5.1f}%] | " +
                  f"{storm_phase.upper():12s} | I:{intensity_bar:10s} {intensity:.2f} | " +
                  f"Pos:({center['lat']:6.3f},{center['lon']:7.3f})")
            
            # Send to Event Hub if requested
            if send_to_event_hub:
                try:
                    success = event_hub_sender.send_storm_event(geojson_frame, frame_number)
                    if success:
                        successful_sends += 1
                except Exception as e:
                    print(f"   ⚠️  Event Hub send failed: {str(e)[:80]}")
                        
            # Wait for next frame
            if not accelerated:
                frame_duration = time.time() - frame_start_time
                sleep_time = STORM_CONFIG['frame_interval_seconds'] - frame_duration
                if sleep_time > 0:
                    time.sleep(sleep_time)
            else:
                time.sleep(1)  # Accelerated: 1s between frames
                
        except Exception as e:
            print(f"❌ ERROR on frame {frame_number}: {str(e)[:100]}")
            continue
    
    # Simulation complete summary
    end_time = datetime.now()
    actual_duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*70)
    print("✅ THUNDERSTORM SIMULATION - COMPLETED")
    print("="*70)
    print(f"Generated frames:     {len(generated_frames)}/{TOTAL_FRAMES}")
    print(f"Event Hub sends:      {successful_sends}/{len(generated_frames)}")
    print(f"Success rate:         {(successful_sends/len(generated_frames)*100):.1f}%")
    print(f"Actual duration:      {actual_duration:.1f}s")
    print(f"Start:                {start_time.strftime('%H:%M:%S')}")
    print(f"End:                  {end_time.strftime('%H:%M:%S')}")
    print("="*70 + "\n")
    
    return generated_frames

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Execute Simulation

# CELL ********************

# Run the complete storm simulation
print("🚀 Starting storm simulation...")
print("⏸️  Press Ctrl+C to stop early\n")

try:
    full_simulation_frames = run_storm_simulation(
        send_to_event_hub=True,   # Send to Event Hub
        accelerated=False         # Real-time simulation (2s per frame)
    )
    print(f"\n✅ Simulation generated {len(full_simulation_frames)} GeoJSON frames")
except KeyboardInterrupt:
    print("\n⚠️  Simulation stopped by user")
except Exception as e:
    print(f"\n❌ Simulation error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
