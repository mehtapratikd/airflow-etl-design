import os


def get_airflow_context_vars(context) -> dict[str, str]:
    """
    :param context: The context for the task_instance of interest.
    """
    return {
        # S3
        "S3_HOST": os.getenv('S3_HOST', 'http://host.docker.internal:9000'),
        "S3_REGION": os.getenv('S3_REGION', 'us-east-1'),
        "S3_AWS_ACCESS_KEY_ID": os.getenv('S3_AWS_ACCESS_KEY_ID', 'admin'),
        "S3_AWS_ACCESS_KEY_SECRET": os.getenv('S3_AWS_ACCESS_KEY_SECRET', 'password'),

        # Telemetry bucket
        "TELEMETRY_BUCKET_NAME": os.getenv('TELEMETRY_BUCKET_NAME', 'data'),
        "TELEMETRY_FILEPATH": os.getenv('TELEMETRY_FILEPATH', 'raw/telemetry'),
        "TELEMETRY_FILENAME": os.getenv('TELEMETRY_FILENAME', 'telemetry.parquet'),

        # Device metadata bucket
        "DEVICE_METADATA_BUCKET_NAME": os.getenv('DEVICE_METADATA_BUCKET_NAME', 'data'),
        "DEVICE_METADATA_FILEPATH": os.getenv('DEVICE_METADATA_FILEPATH', 'raw/config'),
        "DEVICE_METADATA_FILENAME": os.getenv(
            'DEVICE_METADATA_FILENAME', 'device_metadata.json')
    }
