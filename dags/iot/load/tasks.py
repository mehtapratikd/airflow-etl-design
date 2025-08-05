import pandas as pd
from airflow.sdk import task


@task()
def load_device_metadata(data: pd.DataFrame) -> None:
    return
