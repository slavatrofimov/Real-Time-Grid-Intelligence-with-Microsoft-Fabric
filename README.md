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

## ✨ Key Features

- **🚀 One-Click Deployment**: Automated installation notebook deploys all Fabric items with dependency management
- **📊 Real-Time Streaming**: Azure Event Hubs ingestion with Eventstreams for AMI, vehicle, and weather data
- **🔍 Advanced Analytics**: KQL queries for time-series analysis, anomaly detection, and correlation
- **📈 Interactive Dashboards**: Pre-built Power BI reports and KQL dashboards for operational monitoring
- **🎭 Realistic Simulation**: Comprehensive data simulators with seasonal patterns, outages, and failure scenarios
- **🏗️ Scalable Architecture**: Built on Microsoft Fabric's lakehouse and eventhouse architecture
- **🤖 AI-Ready**: Includes Fabric Data Agent integration for natural language queries and incorporates Smart Narratives to facilitate interpretation of Power BI reports

## 🏛️ Solution Architecture

### High-Level Architectural Diagram
![High-Level Solution Architecture](/media/RTI-Grid-Intelligenec-Solution-Diagram.png)

Note that this diagram represents a hypothetical real-world solution. This solution accelerator replaces source systems with Spark notebooks that generate synthetic data.

### Component Details

#### Data Generation
- **AMI Simulators**: Generate realistic smart meter telemetry with seasonal/daily patterns, failures, and outage scenarios
- **Vehicle Simulator**: Simulate routes for field service vehicles with speed and heading data
- **Storm Simulator**: Create weather events that trigger correlated grid outages

#### Ingestion
- **Eventstreams**: Fabric-native streaming connectors with built-in Event Hub endpoints.

#### Storage
- **Eventhouse (KQL Database)**: Hot path for real-time queries with minimal latency
- **Lakehouse (Delta Tables)**: Storage for reference data with seamles integration with the eventhouse

#### Analytics
- **KQL Queries**: Time-series analysis, aggregations, and correlation queries
- **Power BI Semantic Models**: DirectQuery mode connection to the eventhouse for real-time reporting
- **Data Agent**: AI-powered natural language interface to query grid data

#### Action
- **Activator**: Event data is continuously analyzed and automated notification or actions are initiated when triger conditions are satisfied.

## 📦 Prerequisites

### Required
- **Microsoft Fabric Capacity**: F16 or higher recommended (Power BI Premium capacity is also supported). Note: this solution includes AI features that are not available on a Fabric Trial capacity. While you will be albe to deploy the solution, to a workspace on a Trial capacity, some portions of this solution will not work properly.
- **Automatic Page Refresh enabled**: Fabric and Power BI capacities must be configured to allow frequent automatic page refresh. Certain reports included in this solution are configured to refresh pages every 2 seconds to visualize streaming data. [Ensure that your capacity settings allow page refresh with this frequency](https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-automatic-page-refresh#restrictions-on-refresh-intervals).
- **Fabric Workspace**: A workspace with contributor or admin permissions
- **Power BI License**: Power BI Pro or Power BI Premium Per User license.


### Recommended Knowledge
- Basic understanding of Microsoft Fabric concepts (lakehouses, eventhouses, eventstreams)
- Familiarity with KQL (Kusto Query Language) for data exploration (optional)
- Power BI experience for customizing reports (optional)

## 🚀 Installation Instructions

### Step 1: Create Fabric Workspace
1. Log in to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Click **Workspaces** → **+ New workspace**
3. Name your workspace (e.g., "Grid Intelligence")
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

### Step 5: Edit Credentials for Power BI Semantic Models
1. Navigate to the **Visualize and Chat** folder in your workspace
1. Hover over the **Meter Analytics** semantic model and click on the **...** symbol used to access more options.
1. Click on **Settings** in the drop-down menu
1. Expand the section titled "**Data source credentials**
1. Click on the **Edit credentials** link
1. Set "Privacy level setting for this data source" to "Organizational" and click on the **Sign in** button
1. Follow the prompts to complete the Sign-in process with your Entra Id credentials.

[Note: this process needs to be performed for only one of the semantic models, and does not need to be repeated for other semantic models.]

### Step 6: Perform a One-time Refresh of Power BI Semantic Models
1. Navigate to the **Visualize and Chat** folder in your workspace
1. Hover over the **Meter Analytics** semantic model and click on the **⟳** symbol, which will trigger a one-time refresh of reference data in the semantic model.
1. Repeat this process for the remaining semantic models: **Tag Telemetry - Time Series Analysis** and **Vehicle Telemetry - Time Series Analysis**


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
Duration: approximately 2 minutes (triggers outages)
Data Generated: geospatial data representing the progression of a severe thunderstorm through the service area.
```

1. Open the **Storm Simulation** notebook
2. Click **Run all**
3. Observe progression of the storm by opening the **Meter Analytics** report and navigating to the **Meter Outages + Weather** page. Note that storm progression is accelerated -- it will take ~2 minutes for the storm to pass through the service area.
4. Storm events will trigger correlated meter outages in the AMI simulation (when **AMI Telemetry and Outage Simulation** notebook is running)


### Viewing Reports and Dashboards

#### KQL Dashboards

**Meter Statuses Dashboard**
- Real-time meter health monitoring
- Outage maps and timelines
- Power quality monitoring

Access: Navigate to **Visualize and Chat** → **Meter Statuses**

**Vehicle Tracking Dashboard**
- Live vehicle locations on map
- Route history and playback
- Speed and vehicle telemetry metrics

Access: Navigate to **Visualize and Chat** → **Vehicle Tracking**

#### Power BI Reports

**Meter Analytics Report**
- Customer consumption patterns
- Recent trends in power consumption and quality metrics
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
- "What transformer has the highest number of meters impacted by an ougage?"
- "Which vehicle is closest to transformer XYZ?"
- "What's the average power consumption by feeder line and service class?"
- "Which meters have low battery warnings?"

Access: Navigate to **Visualize and Chat** → **Meter_Data_Agent**

### Using Activator (for automated alerts and actions)

The **Meter Activator** enables automatic alerts when trigger conditions are met.

By default, the Activator is configured to generate alerts when the level of total harmonic distortions exceeds a specified threshold and stays at that level for an extended period of time. You may configure other triggers using the no-code authoring interface.

Access: Navigate to **Act** → **Meter Activator**

## 🔧 Troubleshooting
If you encounter challenges with the solution, consider the following steps:
1. Ensure that all pre-requisites have been fully satisfied
1. Ensure that all installation steps have been completed in order
1. Ensure that simulation notebooks are actively running -- it may take a few minutes to simulated data generators to start producing simulated events.
1. Ensure that simulation notebooks are actively running -- by default, meter and vehicle simulations will time out and terminate after 2 hours. Storm simulation will terminate after 2 minutes.

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

Advanced geospatial visualizations in Power BI reports are built using the **Icon Map PRO** visual from [Tekantis](https://www.tekantis.com/). Icon Map Pro is commercially-licensed software. Reach out to Tekantis to license the software for production use cases.


---