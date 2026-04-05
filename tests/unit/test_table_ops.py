import pytest

from aiodynamodb import DynamoModel, table


async def test_create_table_supports_optional_settings_and_delete_table(db):
    @table("events", hash_key="event_id")
    class Event(DynamoModel):
        event_id: str

    response = await db.create_table(
        Event,
        billing_mode="PROVISIONED",
        provisioned_throughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        tags=[{"Key": "env", "Value": "test"}],
        table_class="STANDARD",
    )

    assert response["TableDescription"]["TableName"] == "events"

    delete_response = await db.delete_table(Event)
    assert delete_response["TableDescription"]["TableName"] == "events"


async def test_create_table_rejects_unsupported_key_type(db):
    @table("bad_keys", hash_key="event_id")
    class BadEvent(DynamoModel):
        event_id: bool

    with pytest.raises(TypeError):
        await db.create_table(BadEvent)


def test_table_decorator_rejects_unknown_hash_key():
    with pytest.raises(ValueError, match="hash_key"):

        @table("users", hash_key="nonexistent")
        class Bad(DynamoModel):
            user_id: str


def test_table_decorator_rejects_unknown_range_key():
    with pytest.raises(ValueError, match="range_key"):

        @table("orders", hash_key="order_id", range_key="nonexistent")
        class Bad(DynamoModel):
            order_id: str
            total: int
