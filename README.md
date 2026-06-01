# ADC Signal Airflow Spark Pipeline

Курсовая работа по дисциплине Big Data.

Тема: разработка распределённой системы обработки сигналов АЦП с использованием Apache Airflow, Apache Spark, ClickHouse и Grafana.

## Стек технологий

- Apache Airflow
- Apache Spark Standalone Cluster
- ClickHouse
- Grafana
- Docker Compose
- Python

## Основная идея

Система генерирует цифровые отсчёты сигнала, полученного с АЦП, сохраняет данные в ClickHouse, обрабатывает их в Spark и визуализирует рассчитанные метрики в Grafana.

## Компоненты

- `scripts/data_generator.py` - генерация ADC-сэмплов
- `dags/adc_signal_pipeline.py` - Airflow DAG
- `spark_jobs/adc_signal_processing_job.py` - Spark job
- `clickhouse_init/init.sql` - создание таблиц ClickHouse
- `grafana/` - конфигурация дашбордов