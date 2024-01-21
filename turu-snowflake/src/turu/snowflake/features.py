from typing import TYPE_CHECKING

from turu.core.features import _NotSupportFeature
from typing_extensions import Never, TypeAlias

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
        PyArrowTable = _NotSupportFeature

    else:
        PyArrowTable = pyarrow.Table

except ImportError:
    USE_PYARROW = False
    PyArrowTable = Never  # type: ignore
