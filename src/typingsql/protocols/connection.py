from typing import Protocol

from typingsql.protocols.cursor import CursorProtocol


class ConnectionProtocol(Protocol):
    def cursor(self) -> "CursorProtocol":
        ...
