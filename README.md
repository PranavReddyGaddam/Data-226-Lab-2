# Automated DBT Project for Stock Market Indicators using Airflow and Snowflake

## Overview

This project involves building an automated data pipeline for stock market indicators using dbt, Apache Airflow, Snowflake, and Tableau. The primary objective is to transform and analyze stock data to generate key indicators, leveraging dbt for data transformations, Airflow for automation, and Snowflake as the database. The project also includes visualizations created using Tableau to provide actionable insights from the data.

## Project Steps

### 1. Porting and Setup
- **Pulled Old Python File**: The initial project utilized an existing Python script for data processing.
- **Migration to Airflow**: The entire project was ported from GCP (Google Cloud Platform) to a Docker-based Airflow environment for orchestration and automation of data pipelines.

### 2. DBT Setup
- **New DBT Folder Creation**: Created a new folder named `stock_prices` within the Airflow folder structure for `week8`.
- **Docker Integration**: The `stock_prices` folder was mounted into the `docker_compose_min.yaml` file to ensure accessibility within the Docker container.
- **DBT-Snowflake Configuration**: Added `dbt-snowflake` to the `pip` requirements of the Airflow environment to enable interaction with Snowflake. Configured the `profiles.yml` file within the `stock_prices` folder for Snowflake connectivity.
- **DBT Project Configuration**: Updated `dbt_project.yml` to use incremental models with a composite key of `[date, symbol]` for efficient data updates and deduplication.

### 3. Manual Validation of DBT Models
- All dbt models were manually executed to ensure successful transformations and validate the expected output in Snowflake.

### 4. Automation with Airflow
- **BashOperator Integration**: Automated the execution of dbt models within the Airflow pipeline using the `BashOperator` to perform tasks like running models, testing, and creating snapshots. This ensures that data transformations, validations, and state changes are consistently managed.
- **Airflow DAG Structure**: The pipeline included tasks for:
  - Running dbt models (`dbt run`)
  - Running dbt tests (`dbt test`)
  - Creating dbt snapshots (`dbt snapshot`)

### 5. Data Visualization
- **Tableau Dashboards**: Visualizations were created using Tableau to showcase the calculated stock market indicators, such as RSI, SOC, ADX, DMA, and STR, and provide insights into market trends, volatility, and momentum.

## Project Structure

- **Airflow**: Orchestrates the data pipeline using tasks managed through a DAG (Directed Acyclic Graph).
- **DBT**: Performs data transformations on the `stock_prices` data in Snowflake.
- **Snowflake**: Serves as the data warehouse for storing raw and transformed data.
- **Tableau**: Provides interactive visualizations of the processed data.

## Prerequisites

- **Apache Airflow** installed in a Docker environment.
- **DBT** (Data Build Tool) with `dbt-snowflake` package installed.
- **Snowflake** account with the necessary access and permissions.
- **YFinance API** for stock data extraction.

## How to Use

1. **Setup Docker Environment**:
   - Ensure the `docker_compose_min.yaml` file is configured and the `stock_prices` folder is mounted.
   - Initialize Airflow using:
     ```bash
     docker compose -f docker-compose-min.yaml up airflow-init
     ```
     
     - Run the Airflow service:
     ```bash
     docker compose -f docker-compose-min.yaml up
     ```

2. **Run DBT Models Manually (Optional)**:
   - Navigate to the `stock_prices` dbt folder and run models manually to validate transformations:
     ```bash
     dbt run
     dbt test
     dbt snapshot
     ```

3. **Airflow Automation**:
   - The Airflow DAG for the project handles the automated execution of dbt tasks using `BashOperator`.

4. **Tableau Visualizations**:
   - Connect Tableau to Snowflake and visualize the processed data to gain insights into stock market trends and indicators.

## Key Considerations

- The project ensures data accuracy through testing and snapshots.
- Incremental models with composite keys optimize data processing and deduplication.
- Automation using Airflow simplifies and streamlines data transformation workflows.

## Future Enhancements

- Add more dbt models and indicators to enhance data analysis.
- Integrate alerting mechanisms in Airflow for monitoring pipeline performance.
- Explore advanced visualizations and data storytelling using Tableau.
