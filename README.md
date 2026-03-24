#  🚕 Taxi_Ride_Data_Warehouse_2025

## 📌 Overview
This project builds an end-to-end **data warehouse (lakehouse)** for analyzing taxi trip data from the NYC Taxi and Limousine Commission (TLC) using Spark Declarative Pipelines (SDP) in Databricks.

The pipeline transforms raw trip data into **analytics-ready tables and dashboards**, following the **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 🎯 Objectives
- Build a scalable ETL pipeline using PySpark and SDP
- Clean and validate raw taxi data  
- Design a **star schema data warehouse**  
- Generate business insights through SQL and dashboards  
- Implement **data quality monitoring (bonus feature)**  

---

## 🏗️ Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
<div align="center">
<img src="docs/taxidb_arch.png" width="2500">
</div>

1. Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from PARQUET files in S3 bucket into Databricks.
2. Silver Layer: This layer includes data cleansing, standardization, validation and normalization processes to prepare data for analysis.
3. Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

---

## 🛠️ Important Links & Tools:

- [Datasets](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) : Access to the project dataset (Q1 of 2025).
- [Databricks Free Edition](https://www.databricks.com/learn/free-edition): Learn and try out Databricks with the free edition. 
- [About Spark Declerative Pipelines](https://docs.databricks.com/aws/en/ldp/concepts): Documention to read and learn more info about SDP.
- [Git Repository](https://github.com): Set up a GitHub account and repository to manage, version, and collaborate on your code efficiently.
- [DrawIO](https://www.drawio.com/): Design data architecture, models, flows, and diagrams.
- [dbdiagram.io](https://dbdiagram.io/home): Design ER Digrams

---

## 🔍 Data Quality
### Manage data quality with pipeline expectations
Use expectations to apply quality constraints that validate data as it flows through ETL pipelines. Expectations provide greater insight into data quality metrics and allow you to fail updates or drop records when detecting invalid records.
This ensures:
- transparency  
- better debugging  
- real-world pipeline practices

<div align="center">
<img src="docs/expectations-flow-graph-02ab5dd2011b18ad791c67c0e8449af6.png" width="800">
</div>

[About expectations](https://docs.databricks.com/aws/en/ldp/expectations): Read more about expectations here. 

--- 

## 📊🤖 Data Analysis/AI Use
  
