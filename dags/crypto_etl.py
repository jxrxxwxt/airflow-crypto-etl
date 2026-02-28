import os
import logging
import requests
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# DAG default configuration
default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='crypto_daily_etl',
    default_args=default_args,
    description='Extracts crypto market data, transforms via Pandas, and loads to PostgreSQL',
    start_date=datetime(2026, 2, 27),
    schedule_interval='@daily',
    catchup=False,
    tags=['fintech', 'etl']
)
def crypto_etl_pipeline():

    @task()
    def extract_data(**kwargs):
        """Fetch real-time market data from CoinGecko API."""
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true"
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        return response.json()

    @task()
    def transform_data(raw_data: dict, **kwargs):
        """Convert nested JSON to a structured DataFrame and enforce data types."""
        logical_date = kwargs['ds']
        
        records = []
        for coin, metrics in raw_data.items():
            records.append({
                'logical_date': logical_date,
                'coin_id': coin,
                'price_usd': metrics.get('usd'),
                'market_cap_usd': metrics.get('usd_market_cap'),
                'volume_24h_usd': metrics.get('usd_24h_vol')
            })
        
        df = pd.DataFrame(records)
        
        # Stage the cleaned data locally (Silver layer simulation)
        output_path = f"/opt/airflow/dags/crypto_silver_{logical_date}.csv"
        df.to_csv(output_path, index=False)
        
        logging.info(f"Successfully transformed {len(df)} records. Saved to {output_path}")
        return output_path

    @task()
    def load_data(file_path: str, **kwargs):
        """Load the staged CSV data into PostgreSQL with UPSERT logic for idempotency."""
        # Read the staged data
        df = pd.read_csv(file_path)
        
        # Connect to PostgreSQL (Internal Docker Network)
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        # Define table schema (Silver Layer)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_silver (
                logical_date DATE,
                coin_id VARCHAR(50),
                price_usd NUMERIC(18, 6),
                market_cap_usd NUMERIC(24, 2),
                volume_24h_usd NUMERIC(24, 2),
                PRIMARY KEY (logical_date, coin_id)
            );
        """)
        
        # UPSERT logic: Insert new or update existing based on Primary Key
        insert_query = """
            INSERT INTO crypto_silver (logical_date, coin_id, price_usd, market_cap_usd, volume_24h_usd)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (logical_date, coin_id) 
            DO UPDATE SET 
                price_usd = EXCLUDED.price_usd,
                market_cap_usd = EXCLUDED.market_cap_usd,
                volume_24h_usd = EXCLUDED.volume_24h_usd;
        """
        
        # Convert DataFrame rows to list of tuples for batch execution
        records = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_query, records)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Clean up the staging file to save space
        if os.path.exists(file_path):
            os.remove(file_path)
            
        logging.info(f"Successfully loaded {len(df)} records and cleaned up staging file.")

    # Define the execution pipeline using TaskFlow API
    raw_crypto_data = extract_data()
    staged_file_path = transform_data(raw_crypto_data)
    load_data(staged_file_path)

# Instantiate the DAG
crypto_etl_pipeline()