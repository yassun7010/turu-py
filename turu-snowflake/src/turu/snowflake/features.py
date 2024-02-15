from typing import Generic, TypeVar

from turu.core.features import _NotSupportFeature
from typing_extensions import TypeAlias

T = TypeVar("T")


class _NotSupportFeatureT(Generic[T]):
    pass


try:
    import pandas  # type: ignore[import]  # noqa: F401

    USE_PANDAS = True
    PandasDataFrame: TypeAlias = pandas.DataFrame  # type: ignore
    GenericPandasDataFrame = TypeVar("GenericPandasDataFrame", bound=PandasDataFrame)
    GenericNewPandasDataFrame = TypeVar(
        "GenericNewPandasDataFrame", bound=PandasDataFrame
    )

except ImportError:
    USE_PANDAS = False
    PandasDataFrame: TypeAlias = _NotSupportFeature  # type: ignore
    GenericPandasDataFrame = TypeVar("GenericPandasDataFrame", bound=_NotSupportFeature)
    GenericNewPandasDataFrame = TypeVar(
        "GenericNewPandasDataFrame", bound=_NotSupportFeature
    )


try:
    import pyarrow  # type: ignore[import]  # noqa: F401

    USE_PYARROW = True
    PyArrowTable: TypeAlias = pyarrow.Table  # type: ignore
    GenericPyArrowTable = TypeVar("GenericPyArrowTable", bound=PyArrowTable)
    GenericNewPyArrowTable = TypeVar("GenericNewPyArrowTable", bound=PyArrowTable)


except ImportError:
    USE_PYARROW = False
    PyArrowTable: TypeAlias = _NotSupportFeature  # type: ignore
    GenericPyArrowTable = TypeVar("GenericPyArrowTable", bound=_NotSupportFeature)
    GenericNewPyArrowTable = TypeVar("GenericNewPyArrowTable", bound=_NotSupportFeature)

try:
    from typing import TypeVar

    import pandera  # type: ignore[import]  # noqa: F401

    USE_PANDERA = True
    PanderaDataFrame: TypeAlias = pandera.typing.DataFrame[T]  # type: ignore
    PanderaDataFrameModel = pandera.DataFrameModel  # type: ignore
    GenericPanderaDataFrameModel = TypeVar(
        "GenericPanderaDataFrameModel", bound=pandera.DataFrameModel
    )
    GenericNewPanderaDataFrameModel = TypeVar(
        "GenericNewPanderaDataFrameModel", bound=pandera.DataFrameModel
    )


except ImportError:
    USE_PANDERA = False
    PanderaDataFrame: TypeAlias = _NotSupportFeatureT[T]  # type: ignore
    PanderaDataFrameModel = _NotSupportFeature  # type: ignore
    GenericPanderaDataFrameModel = TypeVar(
        "GenericPanderaDataFrameModel", bound=_NotSupportFeature
    )
    GenericNewPanderaDataFrameModel = TypeVar(
        "GenericNewPanderaDataFrameModel", bound=_NotSupportFeature
    )
