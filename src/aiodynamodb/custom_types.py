import json
from datetime import datetime
from typing import Annotated, Literal

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

__all__ = ("Timestamp", "TimestampMillis", "TimestampMicros", "TimestampNanos", "JSONStr", "KeyT", "ReturnValues")
