from contextlib import asynccontextmanager
from dataclasses import dataclass

import aioboto3

from aiodynamodb.models import DynamoModel

type Key = int | str


@dataclass
class Page[T]:
    items: list[T]
    last_evaluated_key: dict | None = None


class DynamoDB:
    def __init__(self, session: aioboto3.Session | None = None):
        self._session = session or aioboto3.Session()

    @asynccontextmanager
    async def _resource(self):
        async with self._session.resource("dynamodb") as resource:
            yield resource

    async def put(self, item: DynamoModel) -> None:
        async with self._resource() as resource:
            table = await resource.Table(item.Meta.table_name)
            await table.put_item(Item=item.model_dump())

    async def get[T: DynamoModel](
        self,
        model: type[T],
        *,
        hash_key: Key,
        range_key: Key | None = None,
    ) -> T | None:
        meta = model.Meta
        key = {meta.hash_key: hash_key}
        if meta.range_key and range_key is not None:
            key[meta.range_key] = range_key

        async with self._resource() as resource:
            table = await resource.Table(meta.table_name)
            resp = await table.get_item(Key=key)
            item = resp.get("Item")
            if item is None:
                return None
            return model.model_validate(item)
