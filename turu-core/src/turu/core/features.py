from typing_extensions import TypeAlias


class _NotSupportFeature:
    pass


try:
    import pydantic  # type: ignore[import]

    USE_PYDANTIC = True
    PydanticModel: TypeAlias = pydantic.BaseModel  # type: ignore

except ImportError:
    USE_PYDANTIC = False

    PydanticModel: TypeAlias = _NotSupportFeature  # type: ignore
