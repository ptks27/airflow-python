from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# ฟังก์ชันสำหรับดึงข้อมูล
def extract_data(**kwargs):
    url = "https://covid19.ddc.moph.go.th/api/Cases/today-cases-by-provinces"
    response = requests.get(url)
    data = response.json()
    kwargs['ti'].xcom_push(key='covid_data', value=data)

# ฟังก์ชันสำหรับแปลงข้อมูล
def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='covid_data', task_ids='extract')
    df = pd.DataFrame(data)
    
    # บันทึกข้อมูลที่แปลงแล้วลงใน CSV
    output_path = '/opt/airflow/data/covid_cases_by_province.csv'
    df.to_csv(output_path, index=False)
    kwargs['ti'].xcom_push(key='transformed_data_path', value=output_path)

# ฟังก์ชันสำหรับโหลดข้อมูลเข้าฐานข้อมูล PostgreSQL
def load_to_db(db_host, db_name, db_user, db_pswd, db_port, data_path):
    df = pd.read_csv(data_path)
    current_timestamp = datetime.now()
    df['data_ingested_at'] = current_timestamp

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pswd}@{db_host}:{db_port}/{db_name}")
    df.to_sql('covid_cases_by_province', con=engine, schema='data', if_exists='replace', index=False)
    print(f"Success: Loaded {len(df)} recall records to {db_name}.")

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
    'covid_etl',
    default_args=default_args,
    description='A simple ETL DAG for COVID-19 data',
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
        'data_path': '/opt/airflow/data/covid_cases_by_province.csv'
    },
    dag=dag,
)

# กำหนดลำดับของ Task
extract >> transform >> load
