# Real-Time Grid Intelligence with Microsoft Fabric

A comprehensive solution accelerator for power utilities to monitor and optimize electrical grids using Advanced Metering Infrastructure (AMI) data with Microsoft Fabric's real-time intelligence capabilities.

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Solution Architecture](#solution-architecture)
- [Prerequisites](#prerequisites)
- [Installation Instructions](#installation-instructions)
- [Usage Instructions](#usage-instructions)
- [Solution Components](#solution-components)
- [Data Flow](#data-flow)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This solution accelerator demonstrates how power utilities can leverage Microsoft Fabric to build a real-time grid intelligence platform. It processes streaming telemetry from smart meters (AMI), vehicle tracking, and weather data to provide actionable insights for grid operations, outage management, and infrastructure optimization.

The solution simulates a realistic utility grid environment with:
- **Smart meter telemetry** from thousands of meters reporting power consumption, voltage, current, and power quality metrics
- **Vehicle tracking** for field service crews and mobile assets
- **Weather data integration** to correlate environmental conditions with grid performance
- **Real-time intelligence** using KQL (Kusto Query Language) for immediate operational visibility
- **Advanced visualizations** through Power BI reports and real-time dashboards

### Business Value

- **Proactive Outage Management**: Detect and respond to outages in real-time with correlated meter and weather data
- **Grid Optimization**: Identify power quality issues, load imbalances, and infrastructure stress points
- **Operational Efficiency**: Track field service vehicles and optimize crew dispatch
- **Predictive Maintenance**: Detect meter failures, battery issues, and tamper events before they escalate
- **Regulatory Compliance**: Comprehensive data retention and reporting capabilities

## ✨ Key Features

- **🚀 One-Click Deployment**: Automated installation notebook deploys all Fabric items with dependency management
- **📊 Real-Time Streaming**: Azure Event Hubs ingestion with Eventstreams for AMI, vehicle, and weather data
- **🔍 Advanced Analytics**: KQL queries for time-series analysis, anomaly detection, and correlation
- **📈 Interactive Dashboards**: Pre-built Power BI reports and KQL dashboards for operational monitoring
- **🎭 Realistic Simulation**: Comprehensive data simulators with seasonal patterns, outages, and failure scenarios
- **🏗️ Scalable Architecture**: Built on Microsoft Fabric's lakehouse and eventhouse architecture
- **🤖 AI-Ready**: Includes Fabric Data Agent integration for natural language queries and incorporates Smart Narratives to facilitate interpretation of Power BI reports

## 🏛️ Solution Architecture

### High-Level Architecture


### Component Details

#### Data Generation
- **AMI Simulators**: Generate realistic smart meter telemetry with seasonal/daily patterns, failures, and outage scenarios
- **Vehicle Simulator**: Simulate routes for field service vehicles with speed and heading data
- **Storm Simulator**: Create weather events that trigger correlated grid outages

#### Ingestion
- **Eventstreams**: Fabric-native streaming connectors with built-in Event Hub integration
- **Custom Endpoint Source**: EventHub-compatible endpoints for secure data ingestion

#### Storage
- **Eventhouse (KQL Database)**: Hot path for real-time queries with minimal latency
- **Lakehouse (Delta Tables)**: Storage for reference data with seamles integration with the eventhouse

#### Analytics
- **KQL Queries**: Time-series analysis, aggregations, and correlation queries
- **Power BI Semantic Models**: DirectQuery mode connection to the eventhouse for real-time reporting
- **Data Agent**: AI-powered natural language interface to query grid data

## 📦 Prerequisites

### Required
- **Microsoft Fabric Capacity**: F16 or higher recommended (Power BI Premium capacity is also supported). Note: this solution includes AI features that are not available on a Fabric Trial capacity.
- **Fabric Workspace**: A workspace with contributor or admin permissions
- **Fabric License**: Fabric or Power BI Premium Per User license

### Recommended Knowledge
- Basic understanding of Microsoft Fabric concepts (lakehouses, eventhouses, eventstreams)
- Familiarity with KQL (Kusto Query Language) for data exploration (optional)
- Power BI experience for customizing reports (optional)

## 🚀 Installation Instructions

### Step 1: Create Fabric Workspace
1. Log in to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Click **Workspaces** → **+ New workspace**
3. Name your workspace (e.g., "Grid Intelligence Demo")
4. Assign a Fabric capacity or trial capacity
5. Click **Apply**


### Step 2: Download and the Solution Installer notebook
1. Download the [Solution Installer notebook](/deploy/Solution%20Installer.ipynb) to a local folder on your computer.


### Step 3: Import and Run Solution Installer
1. In your Fabric workspace, click **+ New** → **Import notebook**
2. On your local computer, navigate to the folder where you saved the **Solution Installer.ipynb** notebook.
3. Upload and open the **Solution Installer** notebook
4. Click **Run all** to execute the deployment

The installer will:
- ✅ Install required Python packages
- ✅ Download solution files from GitHub
- ✅ Deploy all Fabric items (Eventhouses, Lakehouses, Eventstreams, Notebooks, Reports)
- ✅ Configure item dependencies and relationships
- ✅ Import sample data files


### Step 4: Generate Reference Data

1. Navigate to the **Simulation** folder in your workspace
2. Open the **AMI Reference Data Simulation** notebook
3. Click **Run all**

This generates:
- Meter metadata 
- Network topology (substations, feeder lines, transformers)

### Step 5: Configure Power BI Semantic Models
1. Configure connections for deployed Power BI Semantic models [TODO]
2. Perform a one-time refresh of each Power BI Semantic model [TODO]

## 📖 Usage Instructions

### Running Simulations

#### 1. Start AMI Telemetry Simulation

```
Location: Simulation/AMI Telemetry and Outage Simulation
Duration: 2 hours (configurable)
Data Generated: ~120 batches of telemetry + outage events
```

1. Open the **AMI Telemetry and Outage Simulation** notebook
2. Click **Run all**
3. Monitor progress in the output (status printed every minute)

**Data Generated**:
- Power consumption (kW, voltage (V), current (A), etc.)
- Power quality metrics (power factor, THD, etc.)
- Meter health indicators (battery, tamper detection)
- Outage events (last gasp, restoration)

#### 2. Start Vehicle Tracking Simulation

```
Location: Simulation/Vehicle Telemetry Simulator
Duration: Continuous route playback
Data Generated: GPS coordinates and operational vehicle telemetry every 10 seconds
```

1. Open the **Vehicle Telemetry Simulator** notebook
2. Click **Run all**
3. Vehicles will follow predefined routes with realistic GPS tracking

#### 3. Start Storm Simulation

```
Location: Simulation/Storm Simulation
Duration: Event-driven (triggers outages)
Data Generated: Weather severity data correlated with grid events
```

1. Open the **Storm Simulation** notebook
2. Click **Run all**
3. Storm events will trigger correlated meter outages in the AMI simulation (when **AMI Telemetry and Outage Simulation** notebook is running)

### Exploring Dashboards

#### KQL Dashboards

**Meter Statuses Dashboard**
- Real-time meter health monitoring
- Outage maps and timelines
- Power quality alerts
- Neighborhood-level aggregations

Access: Navigate to **Visualize and Chat** → **Meter Statuses**

**Vehicle Tracking Dashboard**
- Live vehicle locations on map
- Route history and playback
- Speed and telemetry metrics

Access: Navigate to **Visualize and Chat** → **Vehicle Tracking**

#### Power BI Reports

**Meter Analytics Report**
- Customer consumption patterns
- Time-series trends and anomalies
- Outage statuses and weather events across the service area

**Tag Telemetry - Time Series Analysis**
- Multi-meter comparison views
- Flexible time series analysis of metrics from individual meters
- Anomaly detection
- Descriptive statistics and correlation analysis between selected tags

**Vehicle Telemetry - Time Series Analysis**
- Fleet performance metrics
- Vehicle health and performance monitoring

### Using the Data Agent (AI Copilot)

The **Meter_Data_Agent** enables natural language queries:

Example queries:
- "Show me all meters with outages in the last hour"
- "What's the average power consumption by feeder line and service class?"
- "Which meters have low battery warnings?"
- "Find customers with voltage anomalies today"

Access: Navigate to **Visualize and Chat** → **Meter_Data_Agent**


## 🔧 Solution Components

### Eventstreams (Ingestion)
| Name | Purpose | Source | Destination |
|------|---------|--------|-------------|
| **AMI_EventStream** | Smart meter telemetry | Custom Endpoint (Event Hub) | PowerUtilitiesEH.AMITelemetryRaw |
| **Vehicle_EventStream** | GPS tracking data | Custom Endpoint (Event Hub) | PowerUtilitiesEH.VehicleTelemetry |
| **Weather_EventStream** | Weather events | Custom Endpoint (Event Hub) | PowerUtilitiesEH.WeatherData |

### Eventhouse (Real-Time Analytics)
| Database | Tables | Purpose |
|----------|--------|---------|
| **PowerUtilitiesEH** | AMITelemetryRaw, VehicleTelemetry, WeatherData | Hot storage for real-time queries |

### Lakehouse (Reference Data)
| Lakehouse | Tables | Purpose |
|-----------|--------|---------|
| **ReferenceDataLH** | customers, meters, transformers, neighborhoods, settings | Reference data and configuration |

### Notebooks (Simulation)
| Notebook | Purpose |
|----------|---------|
| **AMI Reference Data Simulation** | Generate customer/meter reference data |
| **AMI Telemetry and Outage Simulation** | Stream realistic meter telemetry |
| **Vehicle Telemetry Simulator** | Stream GPS tracking data |
| **Route Simulation** | Generate vehicle route patterns |
| **Storm Simulation** | Create weather events that trigger outages |

### Reports & Dashboards
| Name | Type | Purpose |
|------|------|---------|
| **Meter Statuses** | KQL Dashboard | Real-time operational monitoring |
| **Vehicle Tracking** | KQL Dashboard | Fleet tracking and management |
| **Meter Analytics** | Power BI Report | Historical consumption analysis |
| **Tag Telemetry - Time Series Analysis** | Power BI Report | Multi-meter trend analysis |
| **Vehicle Telemetry - Time Series Analysis** | Power BI Report | Fleet performance analytics |

### Data Agent
| Name | Purpose |
|------|---------|
| **Meter_Data_Agent** | AI-powered natural language interface for querying grid data |

## 🔄 Data Flow

### AMI Telemetry Flow
```
Smart Meters (Simulated)
    → Python Simulator generates telemetry
    → Azure Event Hub (via connection string)
    → Fabric Eventstream (AMI_EventStream)
    → Eventhouse Table (AMITelemetryRaw)
    → KQL Queries & Power BI Reports
```

### Vehicle Tracking Flow
```
Vehicle GPS (Simulated)
    → Python Simulator reads route file
    → Azure Event Hub (via connection string)
    → Fabric Eventstream (Vehicle_EventStream)
    → Eventhouse Table (VehicleTelemetry)
    → KQL Dashboard (Vehicle Tracking)
```

### Reference Data Flow
```
Reference Data Simulator
    → Generates customer/meter data
    → Delta Tables in Lakehouse (ReferenceDataLH)
    → Joined with telemetry for enrichment
    → Power BI Semantic Models
```

### Getting Help

- **Microsoft Fabric Documentation**: [https://learn.microsoft.com/fabric/](https://learn.microsoft.com/fabric/)
- **Community Forums**: [Fabric Community](https://community.fabric.microsoft.com/)
- **GitHub Issues**: Report bugs or request features in this repository

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Contribution Ideas
- Additional simulation scenarios (e.g., solar generation, EV charging)
- New dashboard templates
- Performance optimizations
- Documentation improvements
- Sample KQL queries

## 📄 License

This project is provided as-is for demonstration and educational purposes. 


## Acknowledgments

Built with Microsoft Fabric's powerful real-time intelligence platform:
- **Eventstreams**: For seamless data ingestion
- **Eventhouse**: For lightning-fast KQL queries
- **Lakehouse**: For unified data storage
- **Power BI**: For stunning visualizations
- **Data Activator**: For intelligent alerting
---

For questions or support, please open an issue in the GitHub repository.