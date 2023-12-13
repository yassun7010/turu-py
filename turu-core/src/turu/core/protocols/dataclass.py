from typing import ClassVar, Dict, Protocol


class Dataclass(Protocol):
    __dataclass_fields__: ClassVar[Dict]
