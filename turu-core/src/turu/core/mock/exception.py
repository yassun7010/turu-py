from typing import Optional

from turu.core.exception import TuruError


class TuruMockError(TuruError):
    pass


class TuruMockStoreDataNotFoundError(TuruMockError):
    def __init__(self, counter: int) -> None:
        self.counter = counter

    @property
    def message(self) -> str:
        return f"Mock data not found. counter: {self.counter}"


class TuruMockResponseTypeMismatchError(TuruMockError):
    def __init__(
        self, expected: Optional[type], actual: Optional[type], counter: int
    ) -> None:
        self.expected = expected
        self.actual = actual
        self.counter = counter

    @property
    def message(self) -> str:
        expected_type = self.expected.__name__ if self.expected else None
        actual_type = self.actual.__name__ if self.actual else None

        return f"Mock response type mismatch: Expected {expected_type}, got {actual_type}. counter: {self.counter}"


class TuruMockUnexpectedFetchError(TuruMockError):
    @property
    def message(self) -> str:
        return "Mock fetch is unexpected. use execute_map() or executemany_map() to specify row type."


class TuruCsvHeaderOptionRequiredError(TuruError):
    def __init__(self, row_type: type):
        self.row_type = row_type

    @property
    def message(self) -> str:
        return f'"{self.row_type}" requires header=True when reading from CSV'
