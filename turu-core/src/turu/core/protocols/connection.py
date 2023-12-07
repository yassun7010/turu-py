from typing import Protocol

from turu.core.protocols.cursor import CursorProtocol


class ConnectionProtocol(Protocol):
    def cursor(self) -> "CursorProtocol":
        ...
