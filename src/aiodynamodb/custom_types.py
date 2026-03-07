from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import PlainSerializer

type Timestamp = Annotated[datetime, PlainSerializer(lambda d: int(d.timestamp()))]
