from contextlib import asynccontextmanager

import aioboto3


class DynamoDB(aioboto3.Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    def client(self):
        async with self.resource('dynamodb') as c:
            yield c
