from typing import Optional

from turu.core.exception import TuruError


class TuruMockStoreDataNotFoundError(TuruError):
    @property
    def message(self) -> str:
        return "Mock data not found"


class TuruMockResponseTypeMismatchError(TuruError):
    def __init__(self, expected: Optional[type], actual: Optional[type]) -> None:
        self.expected = expected
        self.actual = actual

    @property
    def message(self) -> str:
        expected_type = self.expected.__name__ if self.expected else None
        actual_type = self.actual.__name__ if self.actual else None

        return (
            f"Mock response type mismatch: Expected {expected_type}, got {actual_type}"
        )


class TuruMockFetchOneSizeError(TuruError):
    def __init__(self, size: int) -> None:
        self.size = size

    @property
    def message(self) -> str:
        return f"Mock fetchone size mismatch: Expected 1, got {self.size}"


class TuruMockUnexpectedFetchError(TuruError):
    @property
    def message(self) -> str:
        return "Mock fetch is unexpected. use execute_map() or executemany_map() to specify row type."
