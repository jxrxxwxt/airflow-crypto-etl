import os
import logging
import requests
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task

def send_discord_alert(context):
    webhook_url = 'https://discord.com/api/webhooks/1477242908053733458/h-bf_mX57sJtAcwn3-3YpdgZBEhQahPX9NCfBvst20v3TLUHbR8VTEE6J-U983wFDyzh' 

    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    exec_date = context.get('execution_date').strftime('%Y-%m-%d %H:%M:%S')
    log_url = task_instance.log_url

    alert_payload = {
        "username": "Airflow DataOps Bot",
        "avatar_url": "https://airflow.apache.org/images/feature-image.png",
        "embeds": [{
            "title": "Airflow Pipeline Alert: Task Failed!",
            "color": 16711680,
            "fields": [
                {"name": "DAG ID", "value": dag_id, "inline": True},
                {"name": "Task ID", "value": task_id, "inline": True},
                {"name": "Execution Time", "value": exec_date, "inline": False},
                {"name": "Log URL", "value": f"[Click here to view logs]({log_url})", "inline": False}
            ]
        }]
    }
    
    response = requests.post(webhook_url, json=alert_payload)
    if response.status_code == 204:
        logging.info("Successfully sent Discord alert.")
    else:
        logging.error(f"Failed to send Discord alert. Status: {response.status_code}")

# DAG default configuration
default_args = {
    'owner': 'data_engineer',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_discord_alert
}

@dag(
    dag_id='crypto_daily_etl',
    default_args=default_args,
    description='Extracts crypto data, transforms, loads to Silver, and models to Gold layer',
    start_date=datetime(2026, 2, 27),
    schedule_interval='@daily',
    catchup=False,
    tags=['fintech', 'etl', 'data-modeling']
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
        output_path = f"/opt/airflow/dags/crypto_silver_{logical_date}.csv"
        df.to_csv(output_path, index=False)
        logging.info(f"Successfully transformed {len(df)} records. Saved to {output_path}")
        return output_path

    @task()
    def load_data(file_path: str, **kwargs):
        """Load the staged CSV data into PostgreSQL Silver Layer with UPSERT logic."""
        df = pd.read_csv(file_path)
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
        cursor = conn.cursor()
        
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
        
        insert_query = """
            INSERT INTO crypto_silver (logical_date, coin_id, price_usd, market_cap_usd, volume_24h_usd)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (logical_date, coin_id) DO UPDATE SET 
                price_usd = EXCLUDED.price_usd,
                market_cap_usd = EXCLUDED.market_cap_usd,
                volume_24h_usd = EXCLUDED.volume_24h_usd;
        """
        records = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_query, records)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        if os.path.exists(file_path):
            os.remove(file_path)
        logging.info("Successfully loaded data to Silver Layer.")

    @task()
    def create_gold_layer(**kwargs):
        """Aggregate data from Silver Layer to Gold Layer for Business Intelligence."""
        logical_date = kwargs['ds']
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
        cursor = conn.cursor()
        
        # Define table schema (Gold Layer)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_gold (
                logical_date DATE PRIMARY KEY,
                btc_price_usd NUMERIC(18, 6),
                eth_price_usd NUMERIC(18, 6),
                btc_eth_market_cap_ratio NUMERIC(10, 4)
            );
        """)
        
        # Advanced SQL: Conditional Aggregation (Pivot) & Math Calculation
        upsert_gold_query = """
            INSERT INTO crypto_gold (logical_date, btc_price_usd, eth_price_usd, btc_eth_market_cap_ratio)
            SELECT 
                logical_date,
                MAX(CASE WHEN coin_id = 'bitcoin' THEN price_usd END) AS btc_price_usd,
                MAX(CASE WHEN coin_id = 'ethereum' THEN price_usd END) AS eth_price_usd,
                MAX(CASE WHEN coin_id = 'bitcoin' THEN market_cap_usd END) / 
                NULLIF(MAX(CASE WHEN coin_id = 'ethereum' THEN market_cap_usd END), 0) AS btc_eth_market_cap_ratio
            FROM crypto_silver
            WHERE logical_date = %s
            GROUP BY logical_date
            ON CONFLICT (logical_date) DO UPDATE SET 
                btc_price_usd = EXCLUDED.btc_price_usd,
                eth_price_usd = EXCLUDED.eth_price_usd,
                btc_eth_market_cap_ratio = EXCLUDED.btc_eth_market_cap_ratio;
        """
        cursor.execute(upsert_gold_query, (logical_date,))
        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Successfully modeled data into Gold Layer.")

    # TaskFlow API execution order
    raw_crypto_data = extract_data()
    staged_file_path = transform_data(raw_crypto_data)
    
    # load_data does not return a value, so we use bitshift operator (>>) to set downstream dependency
    load_data(staged_file_path) >> create_gold_layer()

# Instantiate the DAG
crypto_etl_pipeline()