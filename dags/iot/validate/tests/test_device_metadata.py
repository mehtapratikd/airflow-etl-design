from datetime import timedelta

import pandas as pd
import pendulum

from dags.iot.validate.tasks import validate_device_metadata


def test_validate_device_metadata(device_metadata):
    response = validate_device_metadata.function(device_metadata)
    assert response.shape[0] == 100

def test_invalid_device_id_is_ignored(device_metadata):
    utc_now = pendulum.now('UTC') - timedelta(hours=1)
    invalid_df = pd.DataFrame([{
        "device_id": "device-invalid",
        "temperature_sensor_offset_in_celsius": 1.45,
        "humidity_sensor_offset_in_percent": 3.52,
        "timestamp_iso8601": str(utc_now)
    }])
    new_data = pd.concat([device_metadata, invalid_df], ignore_index=True)
    assert new_data.shape[0] == 101
    response = validate_device_metadata.function(new_data)
    assert response.shape[0] == 100

def test_invalid_temperature_type_is_ignored(device_metadata):
    utc_now = pendulum.now('UTC') - timedelta(hours=1)
    invalid_df = pd.DataFrame([{
        "device_id": "device-000101",
        "temperature_sensor_offset_in_celsius": "string",
        "humidity_sensor_offset_in_percent": 3.52,
        "timestamp_iso8601": str(utc_now)
    }])
    new_data = pd.concat([device_metadata, invalid_df], ignore_index=True)
    assert new_data.shape[0] == 101
    response = validate_device_metadata.function(new_data)
    assert response.shape[0] == 100

def test_invalid_humidity_type_is_ignored(device_metadata):
    utc_now = pendulum.now('UTC') - timedelta(hours=1)
    invalid_df = pd.DataFrame([{
        "device_id": "device-000101",
        "temperature_sensor_offset_in_celsius": 1.45,
        "humidity_sensor_offset_in_percent": "string",
        "timestamp_iso8601": str(utc_now)
    }])
    new_data = pd.concat([device_metadata, invalid_df], ignore_index=True)
    assert new_data.shape[0] == 101
    response = validate_device_metadata.function(new_data)
    assert response.shape[0] == 100

def test_invalid_timestamp_format_is_ignored(device_metadata):
    invalid_df = pd.DataFrame([{
        "device_id": "device-000101",
        "temperature_sensor_offset_in_celsius": 1.45,
        "humidity_sensor_offset_in_percent": -3.5,
        "timestamp_iso8601": "2025-06-01 4:45 PM"
    }])
    new_data = pd.concat([device_metadata, invalid_df], ignore_index=True)
    assert new_data.shape[0] == 101
    response = validate_device_metadata.function(new_data)
    assert response.shape[0] == 100

def test_remove_duplicates(device_metadata):
    utc_now = pendulum.now('UTC') - timedelta(hours=1)
    invalid_df = pd.DataFrame([
        {
            "device_id": "device-000100",
            "temperature_sensor_offset_in_celsius": 1.45,
            "humidity_sensor_offset_in_percent": -3.5,
            "timestamp_iso8601": str(utc_now)
        },
        {
            "device_id": "device-000001",
            "temperature_sensor_offset_in_celsius": 1.45,
            "humidity_sensor_offset_in_percent": -3.5,
            "timestamp_iso8601": str(utc_now)
        },
    ])
    new_data = pd.concat([device_metadata, invalid_df], ignore_index=True)
    assert new_data.shape[0] == 102
    response = validate_device_metadata.function(new_data)
    assert response.shape[0] == 100
