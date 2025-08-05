import logging

import pandas as pd
import pendulum
from airflow.sdk import task


@task()
def transform_device_metadata(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform device metadata

    Specification
    - Temperature offset in degree celcius values is rounded to 2 decimal places
    - Humidity offset in percentage value is rounded to 1 decimal place
    - Add column processed_ts denoting the time of transformation for each device
    """
    task_logger = logging.getLogger("airflow.task")
    data['processed_ts'] = ""
    for idx, row_dict in enumerate(data.to_dict(orient="records")):
        data.at[idx, 'temperature_sensor_offset_in_celsius'] = round(
            row_dict["temperature_sensor_offset_in_celsius"], 2)
        data.at[idx, 'humidity_sensor_offset_in_percent'] = round(
            row_dict["humidity_sensor_offset_in_percent"], 2)
        data.at[idx, 'processed_ts'] = str(pendulum.now('UTC'))
    task_logger.info({
        "stage": "transform",
        "total_records_processed": data.shape[0],
    })
    return data
