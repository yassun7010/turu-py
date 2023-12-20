# from typing import Any, List, Optional, Sequence, Type, cast, override

# import turu.core.cursor
# from google.cloud import bigquery


# class Cursor(turu.core.cursor.Cursor[turu.core.cursor.GenericRowType, Any]):
#     def __init__(
#         self,
#         client: bigquery.Client,
#         *,
#         row_type: Optional[Type[turu.core.cursor.GenericRowType]] = None,
#     ):
#         self._client = client
#         self._row_type = row_type
#         self._arraysize = 1

#     @property
#     def rowcount(self) -> int:
#         return -1

#     @property
#     def arraysize(self) -> int:
#         return self._arraysize

#     @arraysize.setter
#     def arraysize(self, size: int) -> None:
#         self._arraysize = size

#     @override
#     def close(self) -> None:
#         self._client.close()

#     @override
#     def execute(
#         self, operation: str, parameters: Optional[Any] = None, /
#     ) -> "Cursor[Any]":
#         query_job = self._client.query(operation, parameters)
#         self._row_type = None

#         return cast(Cursor, self)

#     @override
#     def executemany(
#         self, operation: str, seq_of_parameters: Sequence[Any], /
#     ) -> "Cursor[Any]":
#         self._client.executemany(operation, seq_of_parameters)
#         self._row_type = None

#         return cast(Cursor, self)

#     @override
#     def execute_map(
#         self,
#         row_type: Type[turu.core.cursor.GenericNewRowType],
#         operation: str,
#         parameters: "Optional[Any]" = None,
#         /,
#     ) -> "Cursor[turu.core.cursor.GenericNewRowType]":
#         self._client.execute(operation, parameters)
#         self._row_type = cast(turu.core.cursor.GenericRowType, row_type)

#         return cast(Cursor, self)

#     @override
#     def executemany_map(
#         self,
#         row_type: Type[turu.core.cursor.GenericNewRowType],
#         operation: str,
#         seq_of_parameters: "Sequence[Any]",
#         /,
#     ) -> "Cursor[turu.core.cursor.GenericNewRowType]":
#         self._client.executemany(operation, seq_of_parameters)
#         self._row_type = cast(turu.core.cursor.GenericRowType, row_type)

#         return cast(Cursor, self)

#     @override
#     def fetchone(self) -> Optional[turu.core.cursor.GenericRowType]:
#         row = self._client.fetchone()
#         if row is None:
#             return None

#         elif self._row_type is not None:
#             return turu.core.cursor.map_row(self._row_type, row)

#         else:
#             return row  # type: ignore[return-value]

#     @override
#     def fetchmany(
#         self, size: Optional[int] = None
#     ) -> List[turu.core.cursor.GenericRowType]:
#         return [
#             turu.core.cursor.map_row(self._row_type, row)
#             for row in self._client.fetchmany(
#                 size if size is not None else self.arraysize
#             )
#         ]

#     @override
#     def fetchall(self) -> List[turu.core.cursor.GenericRowType]:
#         return [
#             turu.core.cursor.map_row(self._row_type, row)
#             for row in self._client.fetchall()
#         ]

#     @override
#     def __next__(self) -> turu.core.cursor.GenericRowType:
#         next_row = self._client.fetchone()
#         if self._row_type is not None and next_row is not None:
#             return turu.core.cursor.map_row(self._row_type, next_row)

#         else:
#             return next_row  # type: ignore[return-value]
