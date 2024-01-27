from typing import TYPE_CHECKING, TypeVar

from turu.core.features import _NotSupportFeature
from typing_extensions import Never, TypeAlias

T = TypeVar("T")

try:
    import pandas  # type: ignore[import]  # noqa: F401

    USE_PANDAS = True
    PandasDataFlame: TypeAlias = pandas.DataFrame  # type: ignore

except ImportError:
    USE_PANDAS = False
    PandasDataFlame: TypeAlias = Never  # type: ignore


try:
    import pyarrow  # type: ignore[import]  # noqa: F401

    USE_PYARROW = True
    if TYPE_CHECKING:
        PyArrowTable: TypeAlias = _NotSupportFeature  # type: ignore

    else:
        PyArrowTable: TypeAlias = pyarrow.Table

except ImportError:
    USE_PYARROW = False
    PyArrowTable: TypeAlias = _NotSupportFeature  # type: ignore

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
    PanderaDataFrame: TypeAlias = _NotSupportFeature[T]  # type: ignore
    PanderaDataFrameModel = _NotSupportFeature[T]  # type: ignore
    GenericPanderaDataFrameModel = TypeVar(
        "GenericPanderaDataFrameModel", bound=_NotSupportFeature
    )
    GenericNewPanderaDataFrameModel = TypeVar(
        "GenericNewPanderaDataFrameModel", bound=_NotSupportFeature
    )
