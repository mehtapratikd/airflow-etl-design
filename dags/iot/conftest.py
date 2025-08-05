import os
from datetime import timedelta

import boto3
import pandas as pd
import pendulum
import pytest

from config.airflow_local_settings import get_airflow_context_vars
from dags.iot.ingest.tests.data_utils import (
    create_device_ids,
    create_device_metadata,
    create_humidity_sensor_telemetry,
    create_temperature_sensor_telemetry,
)


@pytest.fixture(scope="session")
def temperature_telemetry() -> pd.DataFrame:
    df_device_ids = create_device_ids(quantity=100)
    dt = pendulum.now('UTC') - timedelta(hours=1)
    return create_temperature_sensor_telemetry(
        volume=500, devices=df_device_ids, hour=dt.hour, day=dt.day
    )

@pytest.fixture(scope="session")
def humidity_telemetry() -> pd.DataFrame:
    df_device_ids = create_device_ids(quantity=100)
    dt = pendulum.now('UTC') - timedelta(hours=1)
    return create_humidity_sensor_telemetry(
        volume=500, devices=df_device_ids, hour=dt.hour, day=dt.day
    )

@pytest.fixture(scope="session")
def device_metadata() -> pd.DataFrame:
    df_device_ids = create_device_ids(quantity=100)
    dt = pendulum.now('UTC') - timedelta(hours=1)
    return create_device_metadata(
        devices=df_device_ids, hour=dt.hour, day=dt.day
    )

@pytest.fixture(scope="session")
def s3_client():
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("AIRFLOW_CTX_S3_HOST"),
        region_name=os.getenv("AIRFLOW_CTX_S3_REGION"),
        aws_access_key_id=os.getenv("AIRFLOW_CTX_S3_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AIRFLOW_CTX_S3_AWS_ACCESS_KEY_SECRET")
    )
    return s3_client

def pytest_configure(config):
    """Loads environment variables from a JSON file."""
    env_config = get_airflow_context_vars(None)

    for key, value in env_config.items():
        os.environ["AIRFLOW_CTX_" + key] = str(value) # Ensure values are strings
