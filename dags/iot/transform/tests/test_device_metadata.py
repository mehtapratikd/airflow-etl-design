from dags.iot.transform.tasks import transform_device_metadata


def test_transform_data(device_metadata):
    response = transform_device_metadata.function(device_metadata)
    row = response.iloc[0]
    assert (row["temperature_sensor_offset_in_celsius"] * 100).is_integer()
    assert (row["humidity_sensor_offset_in_percent"] * 100).is_integer()
    assert row["processed_ts"] is not None
    assert row["processed_ts"] != ""
    assert response.shape[0] == 100
