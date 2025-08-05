from datetime import timedelta


def get_telemery_dirpath(logical_date) -> str:
    run_dt = logical_date - timedelta(hours=1)
    return run_dt.format("YYYY-MM-DD") + "/" + str(run_dt.hour)

def get_device_metadata_dirpath(logical_date) -> str:
    run_dt = logical_date - timedelta(hours=1)
    return str(run_dt.start_of('week').day)
