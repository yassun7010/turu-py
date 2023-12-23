# NOTE: It's a draft. It's NOT ready for production use.

from typing import Protocol

from turu.core.protocols.cursor import CursorProtocol


class AsyncConnectionProtocol(Protocol):
    async def close(self) -> None:
        ...

    async def commit(self) -> None:
        ...

    async def rollback(self) -> None:
        ...

    def cursor(self) -> CursorProtocol:
        ...