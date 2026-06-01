CREATE DATABASE IF NOT EXISTS adc_signals;

CREATE TABLE IF NOT EXISTS adc_signals.raw_adc_samples
(
    event_time DateTime64(3),
    device_id String,
    channel_id String,
    sample_index UInt64,
    adc_resolution UInt8,
    reference_voltage Float64,
    adc_value UInt16,
    voltage Float64,
    signal_mode String
)
ENGINE = MergeTree
ORDER BY (event_time, device_id, channel_id, sample_index);

CREATE TABLE IF NOT EXISTS adc_signals.signal_window_metrics
(
    window_start DateTime,
    device_id String,
    channel_id String,
    samples_count UInt64,
    mean_voltage Float64,
    rms_voltage Float64,
    min_voltage Float64,
    max_voltage Float64,
    peak_to_peak Float64,
    noise_level Float64,
    snr_db Float64,
    clipping_count UInt64,
    quality_label String
)
ENGINE = MergeTree
ORDER BY (window_start, device_id, channel_id);

CREATE TABLE IF NOT EXISTS adc_signals.signal_anomalies
(
    detected_at DateTime,
    device_id String,
    channel_id String,
    anomaly_type String,
    metric_name String,
    metric_value Float64,
    description String
)
ENGINE = MergeTree
ORDER BY (detected_at, device_id, channel_id);