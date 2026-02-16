from contextlib import asynccontextmanager

import aioboto3


class DynamoDB(aioboto3.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    def client(self):
        async with self.resource('dynamodb') as c:
            yield c

    def scan[T: DynamoBaseModel](self, model: type[T]):
        pass

    def query[T: DynamoBaseModel](self, model: type[T]):
        pass

    def get[T: DynamoBaseModel](self, model: type[T]) -> T:
        pass

    def put[T: DynamoBaseModel](self, model: T):
        pass