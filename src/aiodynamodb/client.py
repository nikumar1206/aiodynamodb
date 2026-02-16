from contextlib import asynccontextmanager
from aiodynamodb.models import DynamoBaseModel
from aiodynamodb._seriaizers import SERIALIZER
from types_aiobotocore_dynamodb.client import DynamoDBClient
from typing import AsyncContextManager

import aioboto3

type Key = int | str


class Page[T]:
    next_key: str
    items;
    list[T]


class DynamoDB(aioboto3.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    async def client(self) -> AsyncContextManager[DynamoDBClient]:
        c: DynamoDBClient
        async with self.resource('dynamodb') as c:
            yield c

    async def scan[H: Key, R: Key, T: DynamoBaseModel](
            self,
            table: str,
            model: type[T]
    ) -> Page[T]:
        pass

    async def query[H: Key, R: Key, T: DynamoBaseModel](
            self,
            table: str,
            hash_key: str,
            hash_key_value: H,
            model: type[T]
    ) -> Page[T]:
        pass

    async def get[H: Key, R: Key, T: DynamoBaseModel](
            self,
            table: str,
            hash_key: str,
            hash_key_value: H,
            range_key: str | None,
            range_key_value: R | None,
            model: type[T]
    ) -> T | None:
        c: DynamoDBClient
        key = {hash_key: SERIALIZER(hash_key_value)}
        if range_key:
            key[range_key] = SERIALIZER(range_key_value)
        async with self.client() as c:
            return await c.get_item(
                TableName=table,
                Key=key
            )

    async def put[H: Key, R: Key, T: DynamoBaseModel](
            self,
            table: str,
            model: T
    ) -> None:
        c: DynamoDBClient
        key = {hash_key: SERIALIZER(hash_key_value)}
        if range_key:
            key[range_key] = SERIALIZER(range_key_value)
        async with self.client() as c:
            await c.put_item(
                TableName=table,
                Item=model.model_dump()
            )

    async def delete[H: Key, R: Key](
            self,
            table: str,
            hash_key: str,
            hash_key_value: H,
            range_key: str | None,
            range_key_value: R | None
    ) -> None:
        c: DynamoDBClient
        key = {hash_key: SERIALIZER(hash_key_value)}
        if range_key:
            key[range_key] = SERIALIZER(range_key_value)
        async with self.client() as c:
            await c.delete_item(
                TableName=table,
                Key=key
            )
