from contextlib import asynccontextmanager
from aiodynamodb.models import DynamoBaseModel

import aioboto3


class DynamoDB(aioboto3.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    def client(self):
        async with self.resource('dynamodb') as c:
            yield c

    def scan[T: DynamoBaseModel](self, table: str, model: type[T]):
        pass

    def query[H, R, T: DynamoBaseModel](self, table: str, hash_key: H, range_key: R | None, model: type[T]) -> T:
        pass

    def get[H, R, T: DynamoBaseModel](self, table: str, hash_key: H, range_key: R | None, model: type[T]) -> T:
        pass

    def put[T: DynamoBaseModel](self, table: str, model: T):
        pass

    def delete[H, R](self, table: str, hash_key: H, range_key: R | None):
        pass