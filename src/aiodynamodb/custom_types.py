import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING, Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, PlainSerializer

_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _timestamp_microseconds(value: datetime) -> int:
    """Return exact microseconds since the Unix epoch.

    ``datetime.timestamp()`` returns a float, which loses precision when it is
    scaled to nanoseconds. Naive datetimes retain Python's usual local-time
    interpretation before conversion to UTC.
    """
    if value.tzinfo is None:
        value = value.astimezone()
    delta = value.astimezone(UTC) - _UNIX_EPOCH
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000) + delta.microseconds


def _truncate_division(value: int, divisor: int) -> int:
    """Divide an integer toward zero, matching ``int(datetime.timestamp())``."""
    if value >= 0:
        return value // divisor
    return -((-value) // divisor)


def _timestamp_seconds(value: datetime) -> int:
    return _truncate_division(_timestamp_microseconds(value), 1_000_000)


def _timestamp_milliseconds(value: datetime) -> int:
    return _truncate_division(_timestamp_microseconds(value), 1_000)


def _timestamp_nanoseconds(value: datetime) -> int:
    return _timestamp_microseconds(value) * 1_000


type Timestamp = Annotated[datetime, PlainSerializer(_timestamp_seconds)]
type TimestampMillis = Annotated[datetime, PlainSerializer(_timestamp_milliseconds)]
type TimestampMicros = Annotated[datetime, PlainSerializer(_timestamp_microseconds)]
type TimestampNanos = Annotated[datetime, PlainSerializer(_timestamp_nanoseconds)]
type DynamoUUID = Annotated[UUID, PlainSerializer(str)]


type JSONStr[T: BaseModel] = Annotated[
    T,
    PlainSerializer(lambda v: v.model_dump_json()),
    BeforeValidator(lambda v: json.loads(v) if isinstance(v, str) else v),
]

type KeyT = int | str | Timestamp | TimestampMillis | TimestampMicros | TimestampNanos | datetime | IntEnum | StrEnum

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
