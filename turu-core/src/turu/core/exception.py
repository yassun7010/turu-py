class TuruException(Exception):
    """Base class for all Turu exceptions."""


class TuruRowTypeError(TuruException):
    """Raised when a row is not of the expected type."""

    def __init__(self, expected: type, actual: type) -> None:
        self.expected = expected
        self.actual = actual

    def __str__(self) -> str:
        return f"Unsupported row type: Expected {self.expected.__name__}, got {self.actual.__name__}."
