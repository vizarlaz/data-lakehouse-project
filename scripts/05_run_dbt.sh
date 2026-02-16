#!bin/bash

echo "Running dbt transformations..."

docker exec lakehouse_airflow bash -c "
    cd /opt/dbt && \
    dbt deps --profiles-dir . && \
    dbt run --profiles-dir . && \
    dbt test --profiles-dir . && \
    dbt docs generate --profiles-dir .
"

echo "dbt transformations complete!"