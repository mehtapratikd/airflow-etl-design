import random

import pandas as pd
import pendulum
import pyarrow as pa
import pyarrow.parquet as pq


def convert_dataframe_to_parquet_bytes(data: pd.DataFrame) -> str:
    table = pa.Table.from_pandas(data)
    buffer_output_stream = pa.BufferOutputStream()
    pq.write_table(table, buffer_output_stream)
    return buffer_output_stream.getvalue().to_pybytes()

def create_device_ids(quantity: int) -> pd.DataFrame:
    if quantity >= 100000:
        raise ValueError('Able to generate less than 100,000 devices')
    devices = []
    for counter in range(1, quantity + 1):
        prefix = 'device-'
        padding = (13 - len(str(counter)) - len(prefix)) * '0'
        device_id = prefix + padding + str(counter)
        devices.append(device_id)
    return pd.DataFrame(devices, columns=['device_id'])

def generate_random_timestamp_for_the_utchour(
        year: int, month: int, day: int, hour: int) -> str:
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    random_microsecond = random.randint(0, 999999)
    return str(pendulum.datetime(
        year=year, month=month, day=day,
        hour=hour, minute=random_minute, second=random_second,
        microsecond=random_microsecond, tz='UTC'
    ))

def create_temperature_sensor_telemetry(
        volume: int, devices: pd.DataFrame, hour: int, day: int) -> pd.DataFrame:
    number_of_devices = devices.shape[0]
    telemetry = []
    utc_now = pendulum.now('UTC')
    for _ in range(0, volume):
        random_device = devices.iloc[random.randint(0, number_of_devices - 1)]
        temperature_sensor_data = {
            "device_id": random_device.device_id,
            "temperature_celsius": round(random.uniform(15.0, 50.0), 4),
            "timestamp_iso8601": generate_random_timestamp_for_the_utchour(
                year=utc_now.year,
                month=utc_now.month,
                day=day,
                hour=hour
            ),
            "sensor_type": "temperature_sensor"
        }
        telemetry.append(temperature_sensor_data)
    return pd.DataFrame(telemetry)

def create_humidity_sensor_telemetry(
        volume: int, devices: pd.DataFrame, hour: int, day: int) -> pd.DataFrame:
    number_of_devices = devices.shape[0]
    telemetry = []
    utc_now = pendulum.now('UTC')
    for _ in range(0, volume):
        random_device = devices.iloc[random.randint(0, number_of_devices - 1)]
        humidity_senso_data = {
            "device_id": random_device.device_id,
            "relative_humidity_percent": round(random.uniform(70.0, 95.0), 2),
            "vpd_kpa": round(random.uniform(0.1, 1.5), 2),
            "timestamp_iso8601": generate_random_timestamp_for_the_utchour(
                year=utc_now.year,
                month=utc_now.month,
                day=day,
                hour=hour
            ),
            "sensor_type": "humidity_sensor"
        }
        telemetry.append(humidity_senso_data)
    return pd.DataFrame(telemetry)

def create_device_metadata(
        devices: pd.DataFrame, hour: int, day: int) -> pd.DataFrame:
    number_of_devices = devices.shape[0]
    devices_metadata = []
    utc_now = pendulum.now('UTC')
    for i in range(0, number_of_devices):
        device = devices.iloc[i]
        device_meta = {
            "device_id": device.device_id,
            "temperature_sensor_offset_in_celsius": round(random.uniform(-1.5, 1.5), 4),
            "humidity_sensor_offset_in_percent": round(random.uniform(-5.0, 5.0), 2),
            "timestamp_iso8601": generate_random_timestamp_for_the_utchour(
                year=utc_now.year,
                month=utc_now.month,
                day=day,
                hour=hour
            ),
        }
        devices_metadata.append(device_meta)
    return pd.DataFrame(devices_metadata)
