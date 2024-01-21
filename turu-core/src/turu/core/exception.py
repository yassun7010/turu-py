from abc import abstractmethod
from typing import Optional


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

    def __init__(self, expected: Optional[type], actual: Optional[type]) -> None:
        self.expected = expected
        self.actual = actual

    @property
    def message(self) -> str:
        expected_type = self.expected.__name__ if self.expected else None
        actual_type = self.actual.__name__ if self.actual else None

        return f"Unsupported row type: Expected {expected_type}, got {actual_type}."


class TuruRowTypeNotSupportedError(TuruError):
    """Raised when a row type is not supported."""

    def __init__(self, row_type: Optional[type]) -> None:
        self.row_type = row_type

    @property
    def message(self) -> str:
        row_type = self.row_type.__name__ if self.row_type else None
        return f"Unsupported row type: {row_type}."


class TuruUnexpectedFetchError(TuruError):
    @property
    def message(self) -> str:
        return "Fetch is unexpected. use execute_*() to specify row type."
