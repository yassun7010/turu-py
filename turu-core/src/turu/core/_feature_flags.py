try:
    import pydantic

    USE_PYDANTIC = True
except ImportError:
    USE_PYDANTIC = False
