from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aioboto3
from boto3.dynamodb.conditions import Key, Attr
from types_aiobotocore_dynamodb.type_defs import TableAttributeValueTypeDef

if TYPE_CHECKING:
    from types_aiobotocore_dynamodb.service_resource import Table
    from types_aiobotocore_dynamodb.client import DynamoDBClient

from aiodynamodb.models import DynamoModel, QueryeResult, Query

type KeyT = int | str


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

    @asynccontextmanager
    async def _client(self):
        async with self._session.client("dynamodb") as client:
            yield client

    async def put[T: DynamoModel](self, item: T) -> None:
        async with self._resource() as resource:
            table: Table = await resource.Table(item.Meta.table_name)
            await table.put_item(Item=item.model_dump())

    async def get[T: DynamoModel](
        self, model: type[T], *, hash_key: KeyT, range_key: KeyT | None = None, consistent_reads: bool = False
    ) -> T | None:
        meta = model.Meta
        key = {meta.hash_key: hash_key}
        if meta.range_key and range_key is not None:
            key[meta.range_key] = range_key

        async with self._resource() as resource:
            table: Table = await resource.Table(meta.table_name)
            resp = await table.get_item(Key=key, ConsistentReads=consistent_reads)
            item = resp.get("Item")
            if item is None:
                return None
            return model.model_validate(item)

    async def query[T: DynamoModel](
        self,
        model: type[T],
        *,
        index_name: str | None = None,
        limit: int | None = None,
        key_condition_expression: Key | None = None,
        filter_expression: Attr | None = None,
        exclusive_start_key: dict[str, TableAttributeValueTypeDef] | None = None,
        return_consumed_capacity=False,
        consistent_read: bool = False,
        scan_index_forward=False,
    ) -> QueryeResult[T]:
        meta = model.Meta

        client: DynamoDBClient
        async with self._client() as client:
            paginator = client.get_paginator("query")

            query_args = dict(
                TableName=meta.table_name,
                IndexName=index_name,
                Limit=limit,
                ScanIndexForward=scan_index_forward,
                ExclusiveStartKey=exclusive_start_key,
                ReturnConsumedCapacity=return_consumed_capacity,
                FilterExpression=filter_expression,
                KeyConditionExpression=key_condition_expression,
                ConsistentRead=consistent_read,
            )
            async for page in paginator.paginate(**query_args):
                yield QueryeResult(
                    items=[model.model_validate(i) for i in page.items], last_evaluated_key=page.last_evaluated_key
                )
