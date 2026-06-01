from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


with DAG(
    dag_id="adc_signal_pipeline",
    description="Big Data pipeline for ADC signal processing with Airflow, Spark and ClickHouse",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["big-data", "adc", "spark", "clickhouse"],
) as dag:

    generate_adc_samples = BashOperator(
        task_id="generate_adc_samples",
        bash_command="python /opt/airflow/scripts/data_generator.py",
    )

    check_raw_samples = BashOperator(
        task_id="check_raw_samples",
        bash_command="python /opt/airflow/scripts/check_clickhouse.py --table raw_adc_samples --min-rows 1",
    )

    unstable_signal_quality_check = BashOperator(
        task_id="unstable_signal_quality_check",
        retries=3,
        retry_delay=timedelta(seconds=20),
        bash_command="python /opt/airflow/scripts/unstable_task.py",
    )

    run_spark_signal_processing = BashOperator(
        task_id="run_spark_signal_processing",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.driver.host=airflow-scheduler \
          /opt/airflow/spark_jobs/adc_signal_processing_job.py
        """,
    )

    check_signal_metrics = BashOperator(
        task_id="check_signal_metrics",
        bash_command="python /opt/airflow/scripts/check_clickhouse.py --table signal_window_metrics --min-rows 1",
    )

    (
        generate_adc_samples
        >> check_raw_samples
        >> unstable_signal_quality_check
        >> run_spark_signal_processing
        >> check_signal_metrics
    )