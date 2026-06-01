# Useful Commands

## Start project

```bash
docker compose up -d --build

Stop project
docker compose down

Check containers
docker compose ps

Check ClickHouse databases
Invoke-WebRequest -UseBasicParsing "http://localhost:8123/?user=default&password=clickhouse&query=SHOW%20DATABASES"

Run generator manually
docker compose exec airflow-scheduler python /opt/airflow/scripts/data_generator.py

Run Spark job manually
docker compose exec airflow-scheduler spark-submit --master spark://spark-master:7077 /opt/airflow/spark_jobs/adc_signal_processing_job.py

Kill one Spark worker
docker stop adc_spark_worker_2

Start Spark worker again
docker start adc_spark_worker_2
---