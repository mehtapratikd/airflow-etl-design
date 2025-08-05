import io
import logging
import os

import boto3
import pandas as pd
import pyarrow.parquet as pq
from airflow.sdk import task
from botocore.exceptions import ClientError

from dags.iot.ingest.utils import get_device_metadata_dirpath, get_telemery_dirpath


@task()
def ingest_telematics(**kwargs) -> pd.DataFrame:
    """
    #### Ingest telemetry from devices
    
    Data is in parquet format and is ingested from S3 bucket
    """
    dirpath = os.getenv("AIRFLOW_CTX_TELEMETRY_FILEPATH", "") + \
                "/" + get_telemery_dirpath(kwargs['logical_date'])
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("AIRFLOW_CTX_S3_HOST", ""),
        region_name=os.getenv("AIRFLOW_CTX_S3_REGION", ""),
        aws_access_key_id=os.getenv("AIRFLOW_CTX_S3_AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.getenv("AIRFLOW_CTX_S3_AWS_ACCESS_KEY_SECRET", "")
    )
    buffer = io.BytesIO()
    try:
        s3_client.download_fileobj(
            os.getenv("AIRFLOW_CTX_TELEMETRY_BUCKET_NAME", ""),
            dirpath + "/" + os.getenv("AIRFLOW_CTX_TELEMETRY_FILENAME", ""),
            buffer
        )
    except ClientError as ex:
        task_logger = logging.getLogger("airflow.task")
        # Additional error details to help debug
        task_logger.critical(ex.response)
        raise ex
    # Seek to the beginning of the buffer before reading
    buffer.seek(0)
    # Read the Parquet file from the buffer
    table = pq.read_table(buffer)
    df = table.to_pandas()
    return df

@task()
def ingest_device_metadata(**kwargs) -> pd.DataFrame:
    """
    #### Ingest device metadata data
    
    Data is in JSON format and is ingested from S3 bucket
    """
    dirpath = os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILEPATH", "") + \
                "/" + get_device_metadata_dirpath(kwargs['logical_date'])
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("AIRFLOW_CTX_S3_HOST", ""),
        region_name=os.getenv("AIRFLOW_CTX_S3_REGION", ""),
        aws_access_key_id=os.getenv("AIRFLOW_CTX_S3_AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.getenv("AIRFLOW_CTX_S3_AWS_ACCESS_KEY_SECRET", "")
    )
    buffer = io.BytesIO()
    task_logger = logging.getLogger("airflow.task")
    try:
        s3_client.download_fileobj(
            os.getenv("AIRFLOW_CTX_DEVICE_METADATA_BUCKET_NAME", ""),
            dirpath + "/" + os.getenv("AIRFLOW_CTX_DEVICE_METADATA_FILENAME", ""),
            buffer
        )
    except ClientError as ex:
        # Additional error details to help debug
        task_logger.critical(ex.response)
        raise ex
    # Seek to the beginning of the buffer before reading
    buffer.seek(0)
    # Read the Parquet file from the buffer
    df = pd.read_json(buffer)
    task_logger.info({
        "stage": "ingest",
        "total_records_processed": df.shape[0]
    })
    return df
