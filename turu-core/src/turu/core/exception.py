from abc import abstractmethod


class TuruException(Exception):
    """Base class for all Turu exceptions."""


class TuruError(TuruException):
    """Raised when an error occurs in Turu."""

    @property
    @abstractmethod
    def message(self) -> str:
        ...

    def __str__(self) -> str:
        return self.message


class TuruRowTypeMismatchError(TuruError):
    """Raised when a row is not of the expected type."""

    def __init__(self, expected: type, actual: type) -> None:
        self.expected = expected
        self.actual = actual

    @property
    def message(self) -> str:
        return f"Unsupported row type: Expected {self.expected.__name__}, got {self.actual.__name__}."


class TuruRowTypeNotSupportedError(TuruError):
    """Raised when a row type is not supported."""

    def __init__(self, row_type: type) -> None:
        self.row_type = row_type

    @property
    def message(self) -> str:
        return f"Unsupported row type: {self.row_type.__name__}."


class TuruUnexpectedFetchError(TuruError):
    @property
    def message(self) -> str:
        return "Fetch is unexpected. use execute_*() to specify row type."
