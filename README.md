# ADC Signal Airflow Spark Pipeline

Курсовая работа по дисциплине **Big Data**.

## Тема

Разработка распределённой системы обработки сигналов АЦП с использованием Apache Airflow, Apache Spark, ClickHouse и Grafana.

## Описание проекта

Проект представляет собой Big Data pipeline для обработки цифровых сигналов, полученных с аналого-цифрового преобразователя. Система генерирует синтетические ADC-сэмплы, сохраняет их в ClickHouse, запускает обработку через Apache Airflow, выполняет распределённые вычисления в Apache Spark и отображает результаты в Grafana.

В качестве исходных данных используются временные ряды цифровых отсчётов АЦП. Для каждого устройства и канала рассчитываются метрики качества сигнала: RMS-напряжение, уровень шума, SNR, peak-to-peak и количество clipping-событий.

## Стек технологий

* Apache Airflow
* Apache Spark Standalone Cluster
* ClickHouse
* Grafana
* PostgreSQL
* Docker Compose
* Python

## Архитектура

```text
ADC Signal Generator
        |
        v
ClickHouse: raw_adc_samples
        |
        v
Apache Airflow DAG
        |
        v
Apache Spark Standalone Cluster
  - spark-master
  - spark-worker-1
  - spark-worker-2
  - spark-worker-3
        |
        v
ClickHouse:
  - signal_window_metrics
  - signal_anomalies
        |
        v
Grafana Dashboard + Alert Rule
```

## Основные компоненты

* `scripts/data_generator.py` — генерация цифровых отсчётов АЦП.
* `scripts/check_clickhouse.py` — проверка наличия данных в ClickHouse.
* `scripts/unstable_task.py` — демонстрация retry-механизма в Airflow.
* `spark_jobs/adc_signal_processing_job.py` — распределённая обработка ADC-сигналов в Spark.
* `dags/adc_signal_pipeline.py` — Airflow DAG для запуска всего пайплайна.
* `clickhouse_init/init.sql` — создание базы данных и таблиц ClickHouse.
* `grafana/dashboards/adc_signal_monitoring.json` — экспортированный dashboard Grafana.

## Запуск проекта

```bash
docker compose up -d --build
```

Проверка контейнеров:

```bash
docker compose ps
```

## Доступные сервисы

* Airflow: http://localhost:8080
* Spark UI: http://localhost:8081
* Grafana: http://localhost:3000
* ClickHouse HTTP: http://localhost:8123

## Логины и пароли

Airflow:

```text
login: admin
password: admin
```

Grafana:

```text
login: admin
password: admin
```

ClickHouse:

```text
user: default
password: clickhouse
database: adc_signals
```

## Airflow DAG

DAG называется:

```text
adc_signal_pipeline
```

Он выполняет следующие шаги:

1. `generate_adc_samples` — генерация ADC-сэмплов.
2. `check_raw_samples` — проверка наличия исходных данных.
3. `unstable_signal_quality_check` — задача, которая специально падает первые попытки и затем успешно завершается после retry.
4. `run_spark_signal_processing` — запуск Spark job.
5. `check_signal_metrics` — проверка результата обработки.

## Spark

Spark запущен в режиме Standalone:

* `spark-master`
* `spark-worker-1`
* `spark-worker-2`
* `spark-worker-3`

Spark job читает данные из ClickHouse, рассчитывает метрики сигнала и записывает результаты обратно в ClickHouse.

## ClickHouse

Основные таблицы:

```sql
adc_signals.raw_adc_samples
adc_signals.signal_window_metrics
adc_signals.signal_anomalies
```

Проверка количества исходных данных:

```bash
curl "http://localhost:8123/?user=default&password=clickhouse&query=SELECT%20count()%20FROM%20adc_signals.raw_adc_samples"
```

Проверка рассчитанных метрик:

```bash
curl "http://localhost:8123/?user=default&password=clickhouse&query=SELECT%20count()%20FROM%20adc_signals.signal_window_metrics"
```

## Grafana

В Grafana реализован dashboard **ADC Signal Monitoring**.

Панели:

* Signal-to-Noise Ratio
* RMS Voltage by ADC Channel
* Noise Level by Channel
* Detected ADC Signal Anomalies

Также создано alert rule:

```text
Low ADC Signal Quality
```

Условие alert:

```text
SNR < 15 dB
```

## Проверка Spark job вручную

```bash
docker compose exec airflow-scheduler spark-submit --master spark://spark-master:7077 /opt/airflow/spark_jobs/adc_signal_processing_job.py
```

## Остановка проекта

```bash
docker compose down
```

Полная очистка данных:

```bash
docker compose down -v
```
