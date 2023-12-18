from typing import Optional

from turu.core.exception import TuruError


class TuruMockStoreDataNotFoundError(TuruError):
    @property
    def message(self) -> str:
        return "Mock data not found"


class TuruMockResponseTypeMismatchError(TuruError):
    def __init__(self, expected: Optional[type], actual: type) -> None:
        self.expected = expected
        self.actual = actual

    @property
    def message(self) -> str:
        return f"Mock response type mismatch: Expected {self.expected.__name__ if self.expected else None}, got {self.actual.__name__}"


class TuruMockFetchOneSizeError(TuruError):
    def __init__(self, size: int) -> None:
        self.size = size

    @property
    def message(self) -> str:
        return f"Mock fetchone size mismatch: Expected 1, got {self.size}"


class TuruMockUnexpectedFetchError(TuruError):
    @property
    def message(self) -> str:
        return "Mock fetch is unexpected. use execute_typing() or executemany_typing() to specify row type."
