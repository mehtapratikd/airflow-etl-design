import pendulum
from airflow.sdk import dag

from dags.iot.ingest.tasks import ingest_telematics


@dag(
    schedule='0 * * * *',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["telemetry", "temperature_sensor", "humidity_sensor"],
)
def telemetry(**kwargs):
    """
    ### ETL of IoT Device Telemetry

    #### Ingest
    Device telematics data is ingested from S3 buckets

    #### Validate
    Valid values for telematics have:
        - Temperature & Humidity values in a acceptable range
        - No null or empty values
        - Timestamp in ISO-8601 format and not in the future
    
    #### Transform

    #### Load
    """
    ingest_telematics()

telemetry()
