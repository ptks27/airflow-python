from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# ฟังก์ชันสำหรับดึงข้อมูล
def extract_data(**kwargs):
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/tickers"
    response = requests.get(url)
    data = response.json()
    kwargs['ti'].xcom_push(key='crypto_data', value=data)

# ฟังก์ชันสำหรับแปลงข้อมูล
def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='crypto_data', task_ids='extract')
    
    if 'tickers' not in data:
        print("Warning: 'tickers' key not found in the API response.")
        tickers = []
    else:
        tickers = data['tickers']
    
    df = pd.DataFrame(tickers)
    
    if df.empty:
        print("Warning: No data available to transform.")
        kwargs['ti'].xcom_push(key='transformed_data_path', value=None)
        return
    
    df = df[['base', 'target', 'last', 'volume', 'converted_last', 'converted_volume', 'timestamp', 'last_traded_at', 'last_fetch_at']]
    
    # แปลง timestamp เป็น datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['last_traded_at'] = pd.to_datetime(df['last_traded_at'])
    df['last_fetch_at'] = pd.to_datetime(df['last_fetch_at'])
    
    # บันทึกข้อมูลที่แปลงแล้วลงใน CSV
    output_path = '/opt/airflow/data/crypto_tickers.csv'
    df.to_csv(output_path, index=False)
    kwargs['ti'].xcom_push(key='transformed_data_path', value=output_path)

# ฟังก์ชันสำหรับโหลดข้อมูลเข้าฐานข้อมูล PostgreSQL
def load_to_db(db_host, db_name, db_user, db_pswd, db_port, data_path, **kwargs):
    if data_path is None or data_path == 'None':
        print("No data available to load. Skipping database insertion.")
        return
    
    try:
        df = pd.read_csv(data_path, parse_dates=['timestamp', 'last_traded_at', 'last_fetch_at'])
    except FileNotFoundError:
        print(f"Error: The file {data_path} was not found. Skipping database insertion.")
        return
    
    if df.empty:
        print("Warning: The CSV file is empty. No data to load into the database.")
        return
    
    current_timestamp = datetime.now()
    df['data_ingested_at'] = current_timestamp

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pswd}@{db_host}:{db_port}/{db_name}")
    df.to_sql('crypto_tickers', con=engine, schema='data', if_exists='replace', index=False)
    print(f"Success: Loaded {len(df)} crypto ticker records to {db_name}.")

# กำหนด default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# กำหนด DAG
dag = DAG(
    'crypto_tickers1_etl',
    default_args=default_args,
    description='A simple ETL DAG for cryptocurrency data',
    schedule_interval=timedelta(days=1),
)

# สร้าง Task
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_to_db,
    op_kwargs={
        'db_host': 'recalls_db',
        'db_name': 'recalls_db',
        'db_user': 'admin',
        'db_pswd': 'admin',
        'db_port': 5432,
        'data_path': "{{ task_instance.xcom_pull(task_ids='transform', key='transformed_data_path') }}"
    },
    provide_context=True,
    dag=dag,
)

# กำหนดลำดับของ Task
extract >> transform >> load
