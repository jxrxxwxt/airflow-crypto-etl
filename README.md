## Automated Crypto Market ETL Pipeline
A production-ready Batch ETL (Extract, Transform, Load) pipeline designed to orchestrate financial data workflows. This project automates the collection of cryptocurrency market data, processes it through a multi-layer architecture, and ensures data integrity and observability.

## Project Overview
This pipeline automates the ingestion of real-time cryptocurrency data (Bitcoin & Ethereum) from the CoinGecko API. It follows the Medallion Architecture to refine raw data into business-ready insights, managed entirely by Apache Airflow and containerized with Docker.

## Data Architecture
The project implements a structured data flow to ensure scalability and reliability:
1. Extract: Fetches raw JSON market data from Public APIs using Python's requests library.
2. Silver Layer (Cleansing):
    - Transforms nested JSON into structured formats using Pandas.
    - Implements Idempotency using UPSERT logic (ON CONFLICT) in PostgreSQL to prevent duplicate records.
3. Gold Layer (Analytics):
    - Performs data modeling using advanced SQL (Conditional Aggregation & Pivoting).
    - Calculates key metrics like the BTC/ETH Market Cap Ratio for immediate Business Intelligence (BI) use.

## Tech Stack
- Orchestration: Apache Airflow
- Data Processing: Python, Pandas
- Database: PostgreSQL
- Infrastructure: Docker & Docker Compose
- Monitoring: Discord Webhook API (Real-time Failure Alerts)
- Version Control: Git

## Key Features
- Automated Scheduling: Fully orchestrated daily runs with Airflow DAGs.
- Data Observability: Custom failure callbacks that send detailed alerts to Discord, including log URLs and task metadata.
- Security First: Sensitive credentials and Webhook URLs are managed via Environment Variables (.env) and excluded from version control.
- Fault Tolerance: Built-in retry mechanisms and database connection handling.

## Getting Started
**Prerequisites**
  - Docker and Docker Compose
  - A Discord Webhook URL (for alerts)

**Installation & Setup**
1. Clone the repository:
    ```
    git clone https://github.com/your-username/airflow-crypto-etl.git
    cd airflow-crypto-etl
    ```
2. Configure Environment Variables:
  
    Create a .env file in the root directory:
    ```
    AIRFLOW_UID=50000
    DISCORD_WEBHOOK_URL=your_discord_webhook_url_here
    ```
3. Build and Start the Containers:
    ```
    docker-compose up -d --build
    ```
4. Access the Dashboard:

    Open http://localhost:8080 (Default Credentials: admin / adminpassword).
