import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, PlainSerializer

type Timestamp = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp()))]
type TimestampMillis = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp() * 1_000))]
type TimestampMicros = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp() * 1_000_000))]
type TimestampNanos = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp() * 1_000_000_000))]


type JSONStr[T: BaseModel] = Annotated[
    T,
    PlainSerializer(lambda v: v.model_dump_json()),
    BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
]

type KeyT = int | str | Timestamp | TimestampMillis | TimestampMicros | TimestampNanos | datetime

type ReturnValues = Literal["NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"]


@dataclass
class _KeyMarker:
    """Annotation marker identifying a field as a table key."""

    kind: str  # "hash" | "range"


if TYPE_CHECKING:
    # Static type checkers see HashKey[str] / RangeKey[str] as just str,
    # so Pylance/mypy won't complain about e.g. `User(user_id="x")`.
    type HashKey[T] = T
    type RangeKey[T] = T
else:
    # At runtime Pydantic needs Annotated[T, marker] so it can discover
    # the marker in field_info.metadata.  __class_getitem__ returns a raw
    # Annotated form that Pydantic correctly decomposes.

    class HashKey:
        """Annotate a field as the table's partition key.

        Usage::

            class User(DynamoModel):
                user_id: HashKey[str]
        """

        def __class_getitem__(cls, item: type) -> Any:
            return Annotated[item, _KeyMarker("hash")]

    class RangeKey:
        """Annotate a field as the table's sort key.

        Usage::

            class Order(DynamoModel):
                order_id: HashKey[str]
                created_at: RangeKey[str]
        """

        def __class_getitem__(cls, item: type) -> Any:
            return Annotated[item, _KeyMarker("range")]


__all__ = (
    "Timestamp",
    "TimestampMillis",
    "TimestampMicros",
    "TimestampNanos",
    "JSONStr",
    "KeyT",
    "ReturnValues",
    "HashKey",
    "RangeKey",
)
