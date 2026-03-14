import pytest

from aiodynamodb import DynamoModel, table


async def test_create_table_supports_optional_settings_and_delete_table(dynamo_resource):
    @table("events", hash_key="event_id")
    class Event(DynamoModel):
        event_id: str

    db = dynamo_resource

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


async def test_create_table_rejects_unsupported_key_type(dynamo_resource):
    @table("bad_keys", hash_key="event_id")
    class BadEvent(DynamoModel):
        event_id: bool

    db = dynamo_resource

    with pytest.raises(TypeError):
        await db.create_table(BadEvent)
