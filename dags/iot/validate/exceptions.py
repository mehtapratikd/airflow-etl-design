import pendulum


class FutureTimestampError(Exception):

    def __init__(self, value: str, current_time: str):
        self.value = value
        self.message = f"Timestamp is in future. Value is {value}. "\
            f"Current time is {current_time}"
        super().__init__(self.message)

class InvalidTimestampError(pendulum.parsing.exceptions.ParserError, ValueError): # pyright: ignore[reportAttributeAccessIssue]

    def __init__(self, value: str):
        self.value = value
        self.message = f"Invalid timestamp. Value is {value}"
        super().__init__(self.message)

class InvalidTemperatureValueError(Exception):

    def __init__(self, value: str):
        self.value = value
        self.message = f"Temperature is not in the acceptable range. "\
            f"Reported value: {value}"
        super().__init__(self.message)

class InvalidDeviceIdError(Exception):

    def __init__(self, value: str):
        self.value = value
        self.message = f"Invalid device id. Value is {value}"
        super().__init__(self.message)

class InvalidDeviceIdLengthError(Exception):

    def __init__(self, value: str, accepted_length: str):
        self.value = value
        self.message = f"Length of device is not {accepted_length}." \
                f"Lenght of device id {value} is: {len(value)}"
        super().__init__(self.message)

class InvalidDeviceTypeError(Exception):

    def __init__(self, value: str):
        self.value = value
        self.message = f"Invalid device type: {value}"
        super().__init__(self.message)

class InvalidHumidityValueError(Exception):

    def __init__(self, value: str):
        self.value = value
        self.message = f"Humidity is not in the acceptable range. "\
            f"Reported value: {value}"
        super().__init__(self.message)
