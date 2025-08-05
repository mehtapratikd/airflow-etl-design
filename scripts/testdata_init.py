"""
# Initialise Test Data

- Load hourly telemetry data into S3 localstack from the past 2 days
"""
from datetime import timedelta

import boto3
import pandas as pa
import pendulum

from dags.iot.ingest.tests.data_utils import (
    convert_dataframe_to_parquet_bytes,
    create_device_ids,
    create_device_metadata,
    create_humidity_sensor_telemetry,
    create_temperature_sensor_telemetry,
)
from dags.iot.ingest.utils import get_device_metadata_dirpath, get_telemery_dirpath

if __name__ == "__main__":
    s3_client = boto3.client(
        's3',
        endpoint_url="http://host.docker.internal:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )
    devices_df = create_device_ids(100)
    utc_now = pendulum.now('UTC')
    dt = utc_now
    for i in range(1, 49):
        temperature_telemetry = create_temperature_sensor_telemetry(
            volume=100, devices=devices_df, hour=dt.hour, day=dt.day
        )
        humidity_telemetry = create_humidity_sensor_telemetry(
            volume=100, devices=devices_df, hour=dt.hour, day=dt.day
        )
        telemetry = pa.concat([
            temperature_telemetry, humidity_telemetry]).sample(frac=1
        )
        path = "raw/telemetry/" + get_telemery_dirpath(dt) + "/telemetry.parquet"
        s3_client.put_object(
            Bucket='data',
            Key=path,
            Body=convert_dataframe_to_parquet_bytes(telemetry)
        )
        dt = utc_now - timedelta(hours=i)
    dt = utc_now - timedelta(hours=1)
    for i in range(1, 3):
        devices_metadata = create_device_metadata(
            devices=devices_df, hour=dt.hour, day=dt.day
        )
        path = "raw/config/" + \
            get_device_metadata_dirpath(dt) + "/device_metadata.json"
        s3_client.put_object(
            Bucket='data',
            Key=path,
            Body=devices_metadata.to_json(orient='records')
        )
        dt = utc_now - timedelta(days=7)
