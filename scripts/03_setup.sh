#!/bin/bash

echo "====================================================================="
echo "  LAKEHOUSE SETUP"
echo "====================================================================="

cd docker

echo "Starting containers..."
docker-compose up -d

echo "Waiting for services..."
sleep 30

echo "Installing Airflow depedencies..."
docker exec lakehouse_airflow pip install --no-cache-dir -r /opt/airflow/requirements.txt

echo "Initializing database..."
docker exec -i lakehouse_postgres psql -U airflow -d airflow < ../scripts/01_init_database.sql

echo "Installing Python in PostgreSQL..."
docker exec lakehouse_postgres apt-get update
docker exec lakehouse_postgres apt-get install -y python3 python3-pip
docker exec lakehouse_postgres pip3 install psycopg2-binary faker

echo "Generating sample data..."
docker cp ../scripts/02_generate_data.py lakehouse_postgres:/tmp/
docker exec lakehouse_postgres python3 /tmp/02_generate_data.py

echo ""
echo "======================================================================="
echo " SETUP COMPLETED"
echo "======================================================================="
echo ""
echo "Airflow: http://localhost:8000"
echo "  User: admin / Pass: admin"
echo ""
echo "MinIO: http://localhost:9001"
echo "  User: minioadmin / Pass: minioadmin"
echo ""
echo "Metabase: http://localhost:3000"
echo "======================================================================="