import os

import pandas as pd
import pendulum
import pyarrow
import pytest
from botocore.exceptions import ClientError

from dags.iot.ingest.tasks import ingest_telematics
from dags.iot.ingest.tests.data_utils import convert_dataframe_to_parquet_bytes
from dags.iot.ingest.utils import get_telemery_dirpath


def test_ingest_flow(s3_client, temperature_telemetry, humidity_telemetry):
    telemetry = pd.concat([temperature_telemetry, humidity_telemetry])
    logical_date = pendulum.now('UTC')
    dirpath = os.getenv("AIRFLOW_CTX_TELEMETRY_FILEPATH", "") \
        + "/"  + get_telemery_dirpath(logical_date)
    s3_client.put_object(
        Bucket=os.getenv("AIRFLOW_CTX_TELEMETRY_BUCKET_NAME"),
        Key=dirpath + "/" + os.getenv("AIRFLOW_CTX_TELEMETRY_FILENAME", ""),
        Body=convert_dataframe_to_parquet_bytes(telemetry)
    )
    context = {
        "logical_date": logical_date,
    }
    df = ingest_telematics.function(**context)
    assert len(df.index) == 1000

def test_for_corrupt_parquet_file(s3_client, temperature_telemetry, humidity_telemetry):
    telemetry = pd.concat([temperature_telemetry, humidity_telemetry])
    logical_date = pendulum.now('UTC')
    dirpath = os.getenv("AIRFLOW_CTX_TELEMETRY_FILEPATH", "") \
          + "/"  + get_telemery_dirpath(logical_date)
    data_in_bytes = convert_dataframe_to_parquet_bytes(telemetry) + b'invalid' # pyright: ignore[reportOperatorIssue]
    s3_client.put_object(
        Bucket=os.getenv("AIRFLOW_CTX_TELEMETRY_BUCKET_NAME"),
        Key=dirpath + "/" + os.getenv("AIRFLOW_CTX_TELEMETRY_FILENAME", ""),
        Body=data_in_bytes
    )
    context = {
        "logical_date": logical_date,
    }
    with pytest.raises(pyarrow.ArrowInvalid):
        ingest_telematics.function(**context)

def test_invalid_s3_bucket_name(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CTX_TELEMETRY_BUCKET_NAME", "invalid")
    logical_date = pendulum.now('UTC')
    context = {
        "logical_date": logical_date,
    }
    with pytest.raises(ClientError):
        ingest_telematics.function(**context)

def test_file_not_available(s3_client):
    logical_date = pendulum.now('UTC')
    dirpath = os.getenv("AIRFLOW_CTX_TELEMETRY_FILEPATH", "") \
          + "/"  + get_telemery_dirpath(logical_date)
    s3_client.delete_object(
        Bucket=os.getenv("AIRFLOW_CTX_TELEMETRY_BUCKET_NAME"),
        Key=dirpath + "/" + os.getenv("AIRFLOW_CTX_TELEMETRY_FILENAME", "")
    )
    context = {
        "logical_date": logical_date,
    }
    with pytest.raises(ClientError):
        ingest_telematics.function(**context)

def test_for_empty_parquet_file(s3_client):
    logical_date = pendulum.now('UTC')
    dirpath = os.getenv("AIRFLOW_CTX_TELEMETRY_FILEPATH", "") \
        + "/"  + get_telemery_dirpath(logical_date)
    s3_client.put_object(
        Bucket=os.getenv("AIRFLOW_CTX_TELEMETRY_BUCKET_NAME"),
        Key=dirpath + "/" + os.getenv("AIRFLOW_CTX_TELEMETRY_FILENAME", ""),
        Body=b''
    )
    context = {
        "logical_date": logical_date,
    }
    with pytest.raises(pyarrow.ArrowInvalid):
        ingest_telematics.function(**context)
