from typing import Generic, TypeVar

from turu.core.features import PydanticModel

GenericTagTarget = TypeVar("GenericTagTarget", bound=PydanticModel)


class Tag:
    pass


class Insert(Tag, Generic[GenericTagTarget]):
    pass


class Update(Tag, Generic[GenericTagTarget]):
    pass


class Delete(Tag, Generic[GenericTagTarget]):
    pass
