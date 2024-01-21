from typing import (
    List,
    Optional,
    Sequence,
    Type,
    cast,
)

import turu.core.async_cursor
import turu.core.cursor
from turu.core.protocols.cursor import Parameters
from turu.core.record.recorder_protcol import RecorderProtcol


class AsyncRecordCursor(
    turu.core.async_cursor.AsyncCursor[turu.core.cursor.GenericRowType, Parameters]
):
    def __init__(
        self,
        recorder: RecorderProtcol,
        cursor: turu.core.async_cursor.AsyncCursor[
            turu.core.cursor.GenericRowType, Parameters
        ],
    ):
        self._recorder = recorder
        self._cursor: turu.core.async_cursor.AsyncCursor = cursor

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self._cursor.arraysize = size

    async def close(self) -> None:
        await self._cursor.close()
        self._recorder.close()

    async def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        self._cursor = await self._cursor.execute(operation, parameters)

        return self

    async def executemany(
        self, operation: str, seq_of_parameters: "Sequence[Parameters]", /
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        self._cursor = await self._cursor.executemany(operation, seq_of_parameters)

        return self

    async def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericNewRowType, Parameters]":
        self._cursor = await self._cursor.execute_map(row_type, operation, parameters)

        return cast(AsyncRecordCursor, self)

    async def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericNewRowType, Parameters]":
        self._cursor = await self._cursor.executemany_map(
            row_type, operation, seq_of_parameters
        )

        return cast(AsyncRecordCursor, self)

    async def fetchone(self) -> Optional[turu.core.cursor.GenericRowType]:
        row = await self._cursor.fetchone()
        if row is not None:
            self._recorder.record([row])

        return row

    async def fetchmany(
        self, size: Optional[int] = None
    ) -> List[turu.core.cursor.GenericRowType]:
        rows = await self._cursor.fetchmany(size)

        self._recorder.record(rows)

        return rows

    async def fetchall(self) -> List[turu.core.cursor.GenericRowType]:
        rows = await self._cursor.fetchall()

        self._recorder.record(rows)

        return rows

    def __iter__(
        self,
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        return self

    async def __anext__(self) -> turu.core.cursor.GenericRowType:
        row = await self._cursor.__anext__()

        self._recorder.record([row])

        return row

    def __getattr__(self, name):
        def _method_missing(*args):
            return args

        return getattr(self._cursor, name, _method_missing)
