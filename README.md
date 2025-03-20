## **Project: Last-Mile Delivery Delays and Route Optimization**

### **1. Project Objectives**
- Build a robust ETL pipeline to process large-scale package delivery and pickup data.
- Standardize and clean location-based data for consistency across multiple cities.
- Perform time-based analysis to extract insights into delivery and pickup efficiency.
- Integrate external data sources (e.g., weather, traffic conditions) to enhance insights.
- Develop a real-time data processing pipeline for monitoring courier performance.

---
## Dataset
The **LaDe Dataset** is a real-world dataset containing:
- **10 million+ package records**.
- Information about delivery times, locations, traffic density, and courier performance.
- Six months of data from diverse regions, including Shanghai and Hangzhou.

## Description
Below is the detailed field of each sub-dataset.
## Last Miles-P [Pickup Download](https://drive.google.com/file/d/1BWZ1Z-Vg0HoKWIuTyrLKzVTuN38fPB3K/view?usp=sharing)
| Data field                 | Description                                  | Unit/format  |
|----------------------------|----------------------------------------------|--------------|
| **Package information**    |                                              |              |
| package_id                 | Unique identifier of each package             | Id           |
| time_window_start          | Start of the required time window             | Time         |
| time_window_end            | End of the required time window               | Time         |
| **Stop information**       |                                              |              |
| lng/lat                    | Coordinates of each stop                      | Float        |
| city                       | City                                         | String       |
| region_id                  | Id of the Region                              | String       |
| aoi_id                     | Id of the AOI (Area of Interest)              | Id           |
| aoi_type                   | Type of the AOI                               | Categorical  |
| **Courier Information**    |                                              |              |
| courier_id                 | Id of the courier                             | Id           |
| **Task-event Information** |                                              |              |
| accept_time                | The time when the courier accepts the task    | Time         |
| accept_gps_time            | The time of the GPS point closest to accept time | Time       |
| accept_gps_lng/lat         | Coordinates when the courier accepts the task | Float        |
| pickup_time                | The time when the courier picks up the task   | Time         |
| pickup_gps_time            | The time of the GPS point closest to pickup_time | Time       |
| pickup_gps_lng/lat         | Coordinates when the courier picks up the task | Float        |
| **Context information**    |                                              |              |
| ds                         | The date of the package pickup                | Date         |


## Last Miles-D [Delivery Download](https://drive.google.com/file/d/1rTe8l68zin2e0QX1sn4HMGjx2bz1e1qz/view?usp=sharing)
| Data field            | Description                          | Unit/format   |
|-----------------------|--------------------------------------|---------------|
| **Package information**   |                                      |               |
| package_id            | Unique identifier of each package     | Id            |
| **Stop information**      |                                      |               |
| lng/lat               | Coordinates of each stop              | Float         |
| city                  | City                                 | String        |
| region_id             | Id of the region                      | Id            |
| aoi_id                | Id of the AOI                         | Id            |
| aoi_type              | Type of the AOI                       | Categorical   |
| **Courier Information**   |                                      |               |
| courier_id            | Id of the courier                     | Id            |
| **Task-event Information**|                                      |               |
| accept_time           | The time when the courier accepts the task | Time      |
| accept_gps_time       | The time of the GPS point whose time is the closest to accept time | Time |
| accept_gps_lng/accept_gps_lat | Coordinates when the courier accepts the task | Float |
| delivery_time         | The time when the courier finishes delivering the task | Time |
| delivery_gps_time     | The time of the GPS point whose time is the closest to the delivery time | Time |
| delivery_gps_lng/delivery_gps_lat | Coordinates when the courier finishes the task | Float |
| **Context information**  |                                      |               |
| ds                    | The date of the package delivery      | Date          |

---

### **2. Data Engineering Pipeline**
The project involves the following steps:

#### **A. Data Ingestion**
- Load CSV files (from multiple cities) into a data warehouse (BigQuery, Snowflake, Redshift).
- Use **Apache Spark** or **Dask** for distributed processing.
- Store raw data in **AWS S3 / Google Cloud Storage**.

#### **B. Data Cleaning & Transformation**
- Handle missing or incorrect location coordinates (`lng/lat`).
- Convert timestamps to a uniform timezone (UTC).
- Normalize categorical fields (`aoi_type`, `region_id`).
- Create derived features:
  - **Pickup Delay** = `pickup_time` - `accept_time`
  - **Delivery Delay** = `delivery_time` - `accept_time`
  - **Distance Covered** = Haversine distance between accept and delivery locations.

#### **C. Data Storage**
- Store cleaned data in a **Data Warehouse (Snowflake, BigQuery, or Redshift)**.
- Optimize storage with **Partitioning (by date/city)** and **Indexing (on package_id, courier_id)**.

#### **D. Data Processing & Streaming (Optional)**
- Implement **Apache Kafka / AWS Kinesis** to stream real-time courier events.
- Use **Apache Flink / Spark Streaming** for near real-time analytics.

#### **E. Data Visualization & Reporting**
- Build **Power BI / Tableau** dashboards to monitor:
  - City-wise delivery time trends.
  - Courier performance metrics.
  - Heatmaps of delivery hotspots.

---

### **3. Key Analytical Insights**
- **Delivery Efficiency**: Identify cities/regions with high delays.
- **Courier Performance**: Analyze which couriers handle deliveries faster.
- **Geospatial Analysis**: Detect patterns in package delivery times based on city infrastructure.
- **Predictive Modeling (ML Extension)**:
  - Predict expected delivery time based on city, time, and distance.
  - Identify couriers at risk of missing deliveries.

---

### **4. Technologies & Tools**
| Component | Tools |
|-----------|------|
| Data Storage | AWS S3, Google Cloud Storage, Snowflake, BigQuery |
| Data Processing | Apache Spark, Dask, Pandas |
| Data Streaming | Apache Kafka, AWS Kinesis, Spark Streaming |
| Data Visualization | Tableau, Power BI, Superset |
| Workflow Orchestration | Apache Airflow, Prefect |
| ML Model (optional) | Scikit-learn, XGBoost |

---

### **5. Potential Extensions**
- **Delivery Route Optimization**: Use historical data to suggest better delivery routes.
- **Fraud Detection**: Detect anomalies in package delivery times.
- **Courier Workload Balancing**: Assign deliveries dynamically based on real-time capacity.
