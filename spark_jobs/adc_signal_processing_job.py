from datetime import datetime

from clickhouse_connect import get_client

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    LongType,
    IntegerType,
    DoubleType,
)


CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
DATABASE = "adc_signals"


def get_clickhouse_client():
    return get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username="default",
        password="clickhouse"
    )


def load_raw_samples(client):
    query = f"""
        SELECT
            event_time,
            device_id,
            channel_id,
            sample_index,
            adc_resolution,
            reference_voltage,
            adc_value,
            voltage,
            signal_mode
        FROM {DATABASE}.raw_adc_samples
        ORDER BY event_time DESC
        LIMIT 6000
    """

    return client.query(query).result_rows


def main():
    client = get_clickhouse_client()
    rows = load_raw_samples(client)

    if not rows:
        raise RuntimeError("No raw ADC samples found in ClickHouse")

    spark = (
        SparkSession.builder
        .appName("ADC Signal Processing Job")
        .getOrCreate()
    )

    schema = StructType([
        StructField("event_time", TimestampType(), False),
        StructField("device_id", StringType(), False),
        StructField("channel_id", StringType(), False),
        StructField("sample_index", LongType(), False),
        StructField("adc_resolution", IntegerType(), False),
        StructField("reference_voltage", DoubleType(), False),
        StructField("adc_value", IntegerType(), False),
        StructField("voltage", DoubleType(), False),
        StructField("signal_mode", StringType(), False),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    df = df.repartition("device_id", "channel_id")

    metrics_df = (
        df
        .withColumn("is_clipping", F.when((F.col("adc_value") <= 0) | (F.col("adc_value") >= 4095), 1).otherwise(0))
        .groupBy(
            F.window("event_time", "1 second").alias("time_window"),
            "device_id",
            "channel_id"
        )
        .agg(
            F.count("*").alias("samples_count"),
            F.avg("voltage").alias("mean_voltage"),
            F.sqrt(F.avg(F.col("voltage") * F.col("voltage"))).alias("rms_voltage"),
            F.min("voltage").alias("min_voltage"),
            F.max("voltage").alias("max_voltage"),
            F.stddev("voltage").alias("noise_level"),
            F.sum("is_clipping").alias("clipping_count")
        )
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("peak_to_peak", F.col("max_voltage") - F.col("min_voltage"))
        .withColumn(
            "snr_db",
            F.when(
                F.col("noise_level") > 0,
                20 * F.log10(F.abs(F.col("rms_voltage")) / F.col("noise_level"))
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "quality_label",
            F.when(F.col("clipping_count") > 0, F.lit("clipping"))
            .when(F.col("snr_db") < 15, F.lit("noisy"))
            .when(F.col("peak_to_peak") < 0.1, F.lit("signal_loss"))
            .otherwise(F.lit("normal"))
        )
        .select(
            "window_start",
            "device_id",
            "channel_id",
            "samples_count",
            "mean_voltage",
            "rms_voltage",
            "min_voltage",
            "max_voltage",
            "peak_to_peak",
            "noise_level",
            "snr_db",
            "clipping_count",
            "quality_label"
        )
        .orderBy("window_start", "device_id", "channel_id")
    )

    metrics_rows = metrics_df.collect()

    output_rows = []
    anomaly_rows = []

    detected_at = datetime.now().replace(microsecond=0)

    for row in metrics_rows:
        output_rows.append([
            row["window_start"],
            row["device_id"],
            row["channel_id"],
            int(row["samples_count"]),
            float(row["mean_voltage"]),
            float(row["rms_voltage"]),
            float(row["min_voltage"]),
            float(row["max_voltage"]),
            float(row["peak_to_peak"]),
            float(row["noise_level"]) if row["noise_level"] is not None else 0.0,
            float(row["snr_db"]) if row["snr_db"] is not None else 0.0,
            int(row["clipping_count"]),
            row["quality_label"],
        ])

        if row["quality_label"] != "normal":
            if row["quality_label"] == "clipping":
                metric_name = "clipping_count"
                metric_value = float(row["clipping_count"])
                description = "ADC clipping was detected in the signal window"
            elif row["quality_label"] == "signal_loss":
                metric_name = "peak_to_peak"
                metric_value = float(row["peak_to_peak"])
                description = "Signal amplitude is too low, possible signal loss"
            else:
                metric_name = "snr_db"
                metric_value = float(row["snr_db"])
                description = "Signal-to-noise ratio is below the threshold"

            anomaly_rows.append([
                detected_at,
                row["device_id"],
                row["channel_id"],
                row["quality_label"],
                metric_name,
                metric_value,
                description
            ])

    client.command(f"TRUNCATE TABLE {DATABASE}.signal_window_metrics")
    client.command(f"TRUNCATE TABLE {DATABASE}.signal_anomalies")

    if output_rows:
        client.insert(
            f"{DATABASE}.signal_window_metrics",
            output_rows,
            column_names=[
                "window_start",
                "device_id",
                "channel_id",
                "samples_count",
                "mean_voltage",
                "rms_voltage",
                "min_voltage",
                "max_voltage",
                "peak_to_peak",
                "noise_level",
                "snr_db",
                "clipping_count",
                "quality_label"
            ]
        )

    if anomaly_rows:
        client.insert(
            f"{DATABASE}.signal_anomalies",
            anomaly_rows,
            column_names=[
                "detected_at",
                "device_id",
                "channel_id",
                "anomaly_type",
                "metric_name",
                "metric_value",
                "description"
            ]
        )

    print(f"Spark default parallelism: {spark.sparkContext.defaultParallelism}")
    print(f"Inserted {len(output_rows)} metric rows")
    print(f"Inserted {len(anomaly_rows)} anomaly rows")

    spark.stop()


if __name__ == "__main__":
    main()