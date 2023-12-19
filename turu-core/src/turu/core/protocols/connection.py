from typing import Protocol

from turu.core.protocols.cursor import CursorProtocol


class ConnectionProtocol(Protocol):
    def close(self) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    def cursor(self) -> CursorProtocol:
        ...
