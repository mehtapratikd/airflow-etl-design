"""
Device Metadata & Telemetry Models

The metadata of devices denote the configuration and calibration parameter of
IoT devices
"""

import pendulum
from pydantic import BaseModel, field_validator

from dags.iot.validate.exceptions import (
    FutureTimestampError,
    InvalidDeviceIdError,
    InvalidDeviceIdLengthError,
    InvalidDeviceTypeError,
    InvalidHumidityValueError,
    InvalidTemperatureValueError,
    InvalidTimestampError,
)


class Device(BaseModel):
    device_id: str
    # Type of device

    @field_validator("device_id")
    def validate_device_id(cls, value: str) -> str:
        if value == "":
            raise InvalidDeviceIdError(value=value)
        if len(value) != 13:
            raise InvalidDeviceIdLengthError(value=str(value), accepted_length="13")
        return value

class DeviceMetadata(Device):
    # Timestamp when the device metadata is captured
    timestamp_iso8601: str
    # Amount of temperature offset based on periodic recalibration of the sensor
    temperature_sensor_offset_in_celsius: float
    # Amount of humidity percentage offset based on periodic recalibration of the sensor
    humidity_sensor_offset_in_percent: float

    @field_validator("timestamp_iso8601")
    def validate_timestamp(cls, value: str) -> str:
        try:
            capture_dt = pendulum.parse(value)
        except InvalidTimestampError:
            raise InvalidTimestampError(value=value)
        current_time = pendulum.now('UTC')
        if capture_dt > current_time: # pyright: ignore[reportOperatorIssue]
            raise FutureTimestampError(value=value, current_time=str(current_time))
        return value

class Sensor(Device):
    # Timestamp when the sensor capture temperature, humidity & vpd values
    timestamp_iso8601: str
    # Unique id of the IoT device
    sensor_type: str


    @field_validator("sensor_type")
    def validate_sensor_type(cls, value) -> str:
        if value not in ["temperature_sensor", "humidity_sensor"]:
            raise InvalidDeviceTypeError(value)
        return value
    
    @field_validator("timestamp_iso8601")
    def validate_timestamp(cls, value: str) -> str:
        try:
            capture_dt = pendulum.parse(value)
        except InvalidTimestampError:
            raise InvalidTimestampError(value=value)
        current_time = pendulum.now('UTC')
        if capture_dt > current_time: # pyright: ignore[reportOperatorIssue]
            raise FutureTimestampError(value=value, current_time=str(current_time))
        return value

class TemperatureSensor(Sensor):
    # It is the average temperature of the air or environment in celcius
    temperature_celsius: float

    @field_validator("temperature_celsius")
    def validate_temperature(cls, value: float) -> float:
        if value > 50.0 or value < -15.0:
            raise InvalidTemperatureValueError(str(value))
        return value

class HumiditySensor(Sensor):
    # It is a measure of the actual amount of water vapor in the air
    # compared to the total amount of vapour that can exist in the air at its 
    # current temperature
    relative_humidity_percent: float
    # It is how much moisture the air can hold during a certain temperature
    vpd_kpa: float

    @field_validator("relative_humidity_percent")
    def validate_relative_humidity_percent(cls, value: float) -> float:
        if value < 20.0 or value > 100.0:
            raise InvalidHumidityValueError(str(value))
        return value

    @field_validator("vpd_kpa")
    def validate_vpd_kpa(cls, value: float) -> float:
        if value < 0.1 or value > 1.5:
            raise InvalidHumidityValueError(str(value))
        return value
