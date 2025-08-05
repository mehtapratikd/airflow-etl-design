import os

import pandas as pd
import pendulum
import pyarrow
import pytest
from botocore.exceptions import ClientError

from dags.iot.ingest.tasks import ingest_device_metadata
from dags.iot.ingest.utils import get_device_metadata_dirpath


def test_ingest_flow(s3_client, device_metadata):
    logical_date = pendulum.now('UTC')
    dirpath = os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILEPATH", "") \
        + "/"  + get_device_metadata_dirpath(logical_date)
    s3_client.put_object(
        Bucket=os.getenv("AIRFLOW_CTX_DEVICE_METADATA_BUCKET_NAME"),
        Key=dirpath + "/" + os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILENAME", ""),
        Body=device_metadata.to_json(orient='records')
    )
    context = {
        "logical_date": logical_date,
    }
    df = ingest_device_metadata.function(**context)
    assert len(df.index) == 100

def test_for_corrupt_json_file(s3_client, device_metadata):
    logical_date = pendulum.now('UTC')
    dirpath = os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILEPATH", "") \
        + "/"  + get_device_metadata_dirpath(logical_date)
    s3_client.put_object(
        Bucket=os.getenv("AIRFLOW_CTX_DEVICE_METADATA_BUCKET_NAME"),
        Key=dirpath + "/" + os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILENAME", ""),
        Body=device_metadata.to_json(orient='records') + 'invalid'
    )
    context = {
        "logical_date": logical_date,
    }
    with pytest.raises(ValueError):
        ingest_device_metadata.function(**context)

def test_invalid_s3_bucket_name(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CTX_DEVICE_METADATA_BUCKET_NAME", "invalid")
    logical_date = pendulum.now('UTC')
    context = {
        "logical_date": logical_date,
    }
    with pytest.raises(ClientError):
        ingest_device_metadata.function(**context)

def test_file_not_available(s3_client):
    logical_date = pendulum.now('UTC')
    dirpath = os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILEPATH", "") \
        + "/"  + get_device_metadata_dirpath(logical_date)
    s3_client.delete_object(
        Bucket=os.getenv("AIRFLOW_CTX_DEVICE_METADATA_BUCKET_NAME"),
        Key=dirpath + "/" + os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILENAME", "")
    )
    context = {
        "logical_date": logical_date,
    }
    with pytest.raises(ClientError):
        ingest_device_metadata.function(**context)

def test_for_empty_json_file(s3_client):
    logical_date = pendulum.now('UTC')
    dirpath = os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILEPATH", "") \
        + "/"  + get_device_metadata_dirpath(logical_date)
    s3_client.put_object(
        Bucket=os.getenv("AIRFLOW_CTX_DEVICE_METADATA_BUCKET_NAME"),
        Key=dirpath + "/" + os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILENAME", ""),
        Body=''
    )
    context = {
        "logical_date": logical_date,
    }
    with pytest.raises(ValueError):
        ingest_device_metadata.function(**context)
