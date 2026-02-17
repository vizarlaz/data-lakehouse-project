#!/bin/bash

echo "====================================================================="
echo "  LAKEHOUSE SETUP (FIXED VERSION)"
echo "====================================================================="

cd docker

echo "Starting containers..."
docker compose up -d

echo "Waiting for services (60s for healthcheck)..."
# Menunggu sampai postgres benar-benar sehat
sleep 60

echo "Installing Airflow dependencies.."

docker exec -u root lakehouse_airflow chmod -R 777 /home/airflow/.local
# Perbaikan: Tambahkan -u airflow agar tidak Permission Denied
docker exec -u airflow lakehouse_airflow python3 -m pip install --user --no-cache-dir -r /opt/airflow/requirements.txt
echo "Initializing database..."
docker exec -i lakehouse_postgres psql -U airflow -d airflow < ../scripts/01_init_database.sql

echo "Installing Python in PostgreSQL..."
# Menambahkan -y agar otomatis 'Yes' saat instalasi
docker exec lakehouse_postgres apt-get update
docker exec lakehouse_postgres apt-get install -y python3 python3-pip postgresql-plpython3-14
# Menggunakan python3 -m pip di Postgres juga lebih aman
docker exec lakehouse_postgres python3 -m pip install --break-system-packages psycopg2-binary faker

echo "Cleaning up old data..."
docker exec -it lakehouse_postgres psql -U airflow -d ecommerce_source -c "TRUNCATE ecommerce.customers, ecommerce.products, ecommerce.orders, ecommerce.order_items RESTART IDENTITY CASCADE;"

echo "Generating sample data..."
docker cp ../scripts/02_generate_data.py lakehouse_postgres:/tmp/
docker exec lakehouse_postgres python3 /tmp/02_generate_data.py

echo ""
echo "======================================================================="
echo " SETUP COMPLETED"
echo "======================================================================="
# ... rest of echo ...