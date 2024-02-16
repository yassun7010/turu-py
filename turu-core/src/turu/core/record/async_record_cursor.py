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
        self.__record_taregt_cursor: turu.core.async_cursor.AsyncCursor = cursor

    @property
    def rowcount(self) -> int:
        return self.__record_taregt_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self.__record_taregt_cursor.arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self.__record_taregt_cursor.arraysize = size

    async def close(self) -> None:
        await self.__record_taregt_cursor.close()
        self._recorder.close()

    async def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        self.__record_taregt_cursor = await self.__record_taregt_cursor.execute(
            operation, parameters
        )

        return self

    async def executemany(
        self, operation: str, seq_of_parameters: "Sequence[Parameters]", /
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        self.__record_taregt_cursor = await self.__record_taregt_cursor.executemany(
            operation, seq_of_parameters
        )

        return self

    async def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericNewRowType, Parameters]":
        self.__record_taregt_cursor = await self.__record_taregt_cursor.execute_map(
            row_type, operation, parameters
        )

        return cast(AsyncRecordCursor, self)

    async def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericNewRowType, Parameters]":
        self.__record_taregt_cursor = await self.__record_taregt_cursor.executemany_map(
            row_type, operation, seq_of_parameters
        )

        return cast(AsyncRecordCursor, self)

    async def fetchone(self) -> Optional[turu.core.cursor.GenericRowType]:
        row = await self.__record_taregt_cursor.fetchone()
        if row is not None:
            self._recorder.record([row])

        return row

    async def fetchmany(
        self, size: Optional[int] = None
    ) -> List[turu.core.cursor.GenericRowType]:
        rows = await self.__record_taregt_cursor.fetchmany(size)

        self._recorder.record(rows)

        return rows

    async def fetchall(self) -> List[turu.core.cursor.GenericRowType]:
        rows = await self.__record_taregt_cursor.fetchall()

        self._recorder.record(rows)

        return rows

    def __iter__(
        self,
    ) -> "AsyncRecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        return self

    async def __anext__(self) -> turu.core.cursor.GenericRowType:
        row = await self.__record_taregt_cursor.__anext__()

        self._recorder.record([row])

        return row

    def __getattr__(self, name):
        def _method_missing(*args):
            return args

        return getattr(self.__record_taregt_cursor, name, _method_missing)
