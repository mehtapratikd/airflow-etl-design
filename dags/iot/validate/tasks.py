import logging

import pandas as pd
from airflow.sdk import task

from dags.iot.validate.data_models import DeviceMetadata


@task()
def validate_telemetry(data: pd.DataFrame):
    """
    #### Validate telemetry data received from temperature sensors

    - Invalid rows are removed. Rows is invalid if data is not available or outside of
    accepted values
    - Duplicate data is ignored. Data is duplicate if the timestamp is same for data
    from the same device
    - Data is also invalid if there is a value type mismatch
    """
    return data


@task()
def validate_device_metadata(data: pd.DataFrame) -> pd.DataFrame:
    """
    #### Validate device metadata

    Specification
    - Validate if device metadata timestamp is in ISO-8601 format
    - Validate temperature and humidity offset are floats
    - Validate if required fields are present.
    - Remove duplicates and consider most recent metadata based on timestamp

    If any of the specifications do not meet the specific device metadata is
    ignored.
    """
    task_logger = logging.getLogger("airflow.task")
    invalid_records_count = 0
    total_records = data.shape[0]
    duplicates_found = 0
    for idx, row_dict in enumerate(data.to_dict(orient="records")):
        try:
            DeviceMetadata(
                device_id=row_dict["device_id"],
                timestamp_iso8601=str(row_dict["timestamp_iso8601"]),
                temperature_sensor_offset_in_celsius=row_dict["temperature_sensor_offset_in_celsius"],
                humidity_sensor_offset_in_percent=row_dict["humidity_sensor_offset_in_percent"]
            )
        except Exception as ex:
            invalid_records_count += 1
            task_logger.error({
                "error_type": "Validation Error",
                "data": row_dict,
                "error_response": str(ex)
            })
            data.drop(idx, inplace=True, axis=0) 
    # Remove duplicates
    data = data.sort_values(by=['device_id', 'timestamp_iso8601'], ascending=True)
    duplicates_found = data.duplicated(subset=['device_id']).sum()
    data = data.drop_duplicates(subset=['device_id'], keep='last')
    task_logger.info({
        "stage": "validate",
        "total_records_processed": total_records,
        "invalid_records_count": invalid_records_count,
        "duplicates_found": int(duplicates_found)
    })
    return data
