import math
import random
from datetime import datetime, timedelta

from clickhouse_connect import get_client


CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123

DATABASE = "adc_signals"
TABLE = "raw_adc_samples"

ADC_RESOLUTION = 12
REFERENCE_VOLTAGE = 3.3
ADC_MAX_VALUE = 2 ** ADC_RESOLUTION - 1

SAMPLE_RATE_HZ = 1000
SAMPLES_PER_CHANNEL = 1000

DEVICES = ["adc_device_1", "adc_device_2", "adc_device_3"]
CHANNELS = ["ch_1", "ch_2"]


def voltage_to_adc(voltage: float) -> int:
    clipped_voltage = max(0.0, min(REFERENCE_VOLTAGE, voltage))
    return int(round((clipped_voltage / REFERENCE_VOLTAGE) * ADC_MAX_VALUE))


def generate_signal_value(t: float, mode: str) -> float:
    base_offset = 1.65
    amplitude = 1.0
    frequency = 50.0

    noise = random.gauss(0, 0.03)

    if mode == "normal":
        return base_offset + amplitude * math.sin(2 * math.pi * frequency * t) + noise

    if mode == "noisy":
        return base_offset + amplitude * math.sin(2 * math.pi * frequency * t) + random.gauss(0, 0.25)

    if mode == "clipping":
        return base_offset + 2.0 * math.sin(2 * math.pi * frequency * t) + noise

    if mode == "signal_loss":
        return base_offset + random.gauss(0, 0.01)

    if mode == "frequency_change":
        return base_offset + amplitude * math.sin(2 * math.pi * 120.0 * t) + noise

    return base_offset + amplitude * math.sin(2 * math.pi * frequency * t) + noise


def main():
    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username="default",
        password="clickhouse"
    )

    start_time = datetime.now().replace(microsecond=0)
    rows = []

    modes = ["normal", "normal", "normal", "noisy", "clipping", "signal_loss", "frequency_change"]

    for device_id in DEVICES:
        for channel_id in CHANNELS:
            signal_mode = random.choice(modes)

            for sample_index in range(SAMPLES_PER_CHANNEL):
                t = sample_index / SAMPLE_RATE_HZ
                event_time = start_time + timedelta(milliseconds=sample_index)

                voltage = generate_signal_value(t, signal_mode)
                adc_value = voltage_to_adc(voltage)

                rows.append([
                    event_time,
                    device_id,
                    channel_id,
                    sample_index,
                    ADC_RESOLUTION,
                    REFERENCE_VOLTAGE,
                    adc_value,
                    float(voltage),
                    signal_mode
                ])

    client.insert(
        f"{DATABASE}.{TABLE}",
        rows,
        column_names=[
            "event_time",
            "device_id",
            "channel_id",
            "sample_index",
            "adc_resolution",
            "reference_voltage",
            "adc_value",
            "voltage",
            "signal_mode"
        ]
    )

    print(f"Inserted {len(rows)} ADC samples into {DATABASE}.{TABLE}")


if __name__ == "__main__":
    main()