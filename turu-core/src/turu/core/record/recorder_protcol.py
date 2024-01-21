from typing import Any, Protocol


class RecorderProtcol(Protocol):
    def record(self, rows: Any) -> Any:
        ...

    def close(self) -> None:
        ...
