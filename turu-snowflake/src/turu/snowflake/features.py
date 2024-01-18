import typing_extensions

try:
    import pandas  # type: ignore[import]  # noqa: F401

    USE_PANDAS = True
    PandasDataFlame = pandas.DataFrame

except ImportError:
    USE_PANDAS = False
    PandasDataFlame = typing_extensions.Never


try:
    import pyarrow  # type: ignore[import]  # noqa: F401

    USE_PYARROW = True
    PyArrowTable = pyarrow.Table

except ImportError:
    USE_PYARROW = False
    PyArrowTable = typing_extensions.Never
