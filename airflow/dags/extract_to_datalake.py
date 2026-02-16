from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import pandas as pd
from minio import Minio
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_table(**context):
    table = context['params']['table']
    execution_date = context['ds_nodash']

    logger.info(f"Extracting {table} for {execution_date}")

    conn = psycopg2.connect(
        host = "postgres",
        database = "ecommerce_source",
        user="airflow",
        password="airflow"
    )

    df = pd.read_sql(f"SELECT * FROM ecommerce.{table}", conn)
    conn.close()

    logger.info(f"Extracted {len(df)} rows from {table}")

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    path= f"raw/ecommerce/{table}/data={execution_date}/{table}.parquet"

    client.put_object(
        bucket_name="datalake",
        object_name=path,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )

    logger.info(f"Uploaded to s3://datalake/{path}")

    return {
        'table': table,
        'rows': len(df),
        'path': path,
        'size_kb': buffer.getbuffer().nbytes / 1024
    }

with DAG(
    'extract_to_datalake',
    default_args=default_args,
    description='Extract from PostgreSQL to Data Lake',
    schedule_interval='0.1 * * *', # 1 AM daily
    catchup=False,
    tags=['extract', 'etl']
) as dag:
    tables = ['customers','products','orders','order_items']

    for table in tables:
        PythonOperator(
            task_id=f'extract_{table}',
            python_callable=extract_table,
            params={'table': table},
            provide_context=True
        )