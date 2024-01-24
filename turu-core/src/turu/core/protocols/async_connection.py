# NOTE: It's a draft. It's NOT ready for production use.

from typing import Protocol

from turu.core.protocols.async_cursor import AsyncCursorProtocol


class AsyncConnectionProtocol(Protocol):
    async def close(self) -> None:
        """Close the connection now."""
        ...

    async def commit(self) -> None:
        """Commit any pending transaction to the database."""
        ...

    async def rollback(self) -> None:
        """Roll back to the start of any pending transaction.

        Closing a connection without committing the changes first
        will cause an implicit rollback to be performed.
        """
        ...

    async def cursor(self) -> AsyncCursorProtocol:
        """Return a new Cursor Object using the connection."""
        ...
