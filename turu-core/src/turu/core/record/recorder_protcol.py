from typing import Any, Protocol, TextIO


class RecorderProtcol(Protocol):
    @property
    def file(self) -> TextIO:
        ...

    def record(self, rows: Any) -> None:
        ...

    def close(self) -> None:
        ...
