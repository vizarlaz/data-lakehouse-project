import psycopg2
from minio import Minio
import pandas as pd
from io import BytesIO
import sys

def load_table(table, date):
    print(f"Loading {table}...")

    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    path = f"raw/ecommerce/{table}/date={date}/{table}.parquet"

    try:
        response = client.get_object("datalake", path)
        df = pd.read_parquet(BytesIO(response.read()))
    except:
        print(f"File not found: {path}")
        return


    conn = psycopg2.connect(
        host="localhost",
        database='airflow',
        user="airflow",
        password="airflow"
    )

    cursor = conn.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS warehouse.{table}")

    df.to_sql(
        table,
        conn,
        schema='warehouse',
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )

    print(f" Loaded {len(df)} rows to warehouse.{table}")

    conn.close()


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "20260210"

    tables = ['customers','products','orders','order_items']

    for table in tables:
        load_table(table, date)

    print("\nAll tables loaded to warehouse!")