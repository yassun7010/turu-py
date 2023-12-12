try:
    import pydantic  # noqa: F401

    USE_PYDANTIC = True
    PydanticModel = pydantic.BaseModel

except ImportError:
    USE_PYDANTIC = False
    PydanticModel = None
