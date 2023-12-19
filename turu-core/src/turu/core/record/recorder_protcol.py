from typing import Any, Protocol


class RecorderProtcol(Protocol):
    def write_row(self, row: Any) -> None:
        ...

    def close(self) -> None:
        ...
