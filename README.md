# Snowflake AQI ETL Data Pipeline

This project demonstrates a complete **data engineering workflow** for building a scalable ETL pipeline using **Snowflake**, **Snowpark**, and **Streamlit**.  
The pipeline ingests **hourly Air Quality Index (AQI) data for Indian cities**, stages and transforms it within Snowflake, and visualizes key environmental insights through interactive dashboards.  

---

## Overview

**Key Capabilities:**  
- Automated data ingestion from a public AQI API (scheduled hourly)  
- Data cleaning, validation, and transformation in **Snowflake** using **SQL** and **Snowpark Python**  
- Aggregated tables optimized for analytics and dashboard queries  
- Interactive **Streamlit dashboards** for real-time AQI monitoring and trend analysis  
- Fully automated workflow managed through **GitHub Actions**  

---

## Repository Structure

| Directory | Description |
|------------|-------------|
| **`Diagrams/`** | Architecture and ETL flow diagrams |
| **`GIT Action Pipeline/`** | GitHub Actions workflows for CI/CD and scheduling |
| **`Raw Data/`** | Sample raw JSONs and reference data used for testing |
| **`Snowpark Python/`** | Python scripts for ingestion and transformation using Snowpark |
| **`SQL Scripts/`** | SQL files for schema creation, staging, cleaning, and aggregation |
| **`Streamlit Scripts/`** | Streamlit applications for visualization and exploration |

---

## GitHub Actions

The scheduled workflow **`GIT Action Pipeline/air_quality_hourly.yml`** automates data ingestion every hour at **:45**.  
It performs the following steps:  
1. Checks out the repository  
2. Sets up dependencies and environment  
3. Runs the ingestion script `ingest-api-data.py` to pull fresh AQI data and load it into Snowflake  

**Required Secrets (GitHub → Repository → Settings → Secrets):**  
- `SNOWFLAKE_ACCOUNT`  
- `SNOWFLAKE_USER`  
- `SNOWFLAKE_PASSWORD`  
- `SNOWFLAKE_WAREHOUSE`  
- `SNOWFLAKE_DATABASE`  
- `SNOWFLAKE_SCHEMA`  
- `API_KEY`  

---

## Diagrams

Located in the `Diagrams/` directory:  
- **`high-level-overview.png`** – Architecture overview from API to dashboard  
- **`snowflake-graph.png`** – Snowflake schema and data flow  
- **`table-schemas-flow.png`** – Logical relationships between staging and cleaned tables  

---

## Notes and Next Steps

- Store all credentials in environment variables or a secret manager instead of in-code placeholders.  
- Extend `ingest-api-data.py` to include validation logs and error handling.  
- Add automated tests to mock API responses and verify Snowpark uploads.  
- Incorporate incremental loads to minimize compute and improve pipeline efficiency.  
- Explore integration with **Snowflake Streams and Tasks** for native orchestration.  

---

## Project Impact

This project reflects how a **modern cloud-based data engineering workflow** can be designed with minimal infrastructure overhead.  
It highlights:  
- Practical application of **Snowflake** and **Snowpark** for large-scale data processing  
- Use of **GitHub Actions** for reliable, automated data pipelines  
- Real-time data visibility through **Streamlit dashboards**  
