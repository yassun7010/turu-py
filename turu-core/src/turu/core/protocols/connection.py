from typing import Protocol

from turu.core.protocols.cursor import CursorProtocol


class ConnectionProtocol(Protocol):
    def close(self) -> None:
        """Close the connection now."""
        ...

    def commit(self) -> None:
        """Commit any pending transaction to the database."""
        ...

    def rollback(self) -> None:
        """Roll back to the start of any pending transaction.

        Closing a connection without committing the changes first
        will cause an implicit rollback to be performed.
        """
        ...

    def cursor(self) -> CursorProtocol:
        """Return a new Cursor Object using the connection."""
        ...
