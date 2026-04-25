Overview

Built an end-to-end data pipeline using PySpark in Databricks following Medallion Architecture (Bronze → Silver → Gold) to process and analyze global energy data.

Architecture
    Raw CSV → Bronze → Silver → Gold
    Bronze: Raw data ingestion
    Silver: Cleaning, type casting, feature engineering
    Gold: Aggregated, analytics-ready dataset


⚙️ Tech Stack
PySpark
Databricks
Delta Lake
Power BI

Project Structure
    energy-data-pipeline/
    │
    ├── data/raw/owid-energy-data.csv
    ├── notebooks/pipeline.py
    ├── README.md

Pipeline Highlights
    Cleaned and standardized energy dataset
    Created energy_category (GREEN / HYBRID / FOSSIL)
    Built aggregated country-level metrics:
    Avg energy consumption
    Avg electricity generation
    Avg renewable share


Key Insights
    Top countries leading in renewable energy
    High energy consumption economies
    Global energy mix (Green vs Fossil vs Hybrid)
    Renewable share vs consumption comparison