import pendulum
from airflow.sdk import dag

from dags.iot.ingest.tasks import ingest_device_metadata
from dags.iot.transform.tasks import transform_device_metadata
from dags.iot.validate.tasks import validate_device_metadata


@dag(
    schedule='0 0 * * 0',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["device_metadata", "temperature_sensor", "humidity_sensor"],
)
def device_metadata(**kwargs):
    """
    ### ETL of IoT Device Metadata
    """
    
    ingested = ingest_device_metadata()
    validated = validate_device_metadata(ingested) # pyright: ignore[reportArgumentType]
    transform_device_metadata(validated) # pyright: ignore[reportArgumentType]


device_metadata()
