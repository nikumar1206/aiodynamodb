import pytest

from aiodynamodb import DynamoModel, HashKey, RangeKey, table


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


# ---------------------------------------------------------------------------
# HashKey / RangeKey annotation discovery
# ---------------------------------------------------------------------------


def test_annotation_hash_key_only():
    @table("t1")
    class M(DynamoModel):
        pk: HashKey[str]
        name: str

    assert M.Meta.hash_key == "pk"
    assert M.Meta.range_key is None


def test_annotation_hash_and_range_key():
    @table("t2")
    class M(DynamoModel):
        pk: HashKey[str]
        sk: RangeKey[str]
        data: int = 0

    assert M.Meta.hash_key == "pk"
    assert M.Meta.range_key == "sk"


def test_string_arg_hash_key_backward_compat():
    @table("t3", hash_key="pk")
    class M(DynamoModel):
        pk: str
        name: str

    assert M.Meta.hash_key == "pk"


def test_string_arg_hash_and_range_backward_compat():
    @table("t4", hash_key="pk", range_key="sk")
    class M(DynamoModel):
        pk: str
        sk: str

    assert M.Meta.hash_key == "pk"
    assert M.Meta.range_key == "sk"


def test_both_annotation_and_string_arg_raises():
    with pytest.raises(TypeError, match="annotation or decorator argument, not both"):

        @table("t5", hash_key="pk")
        class M(DynamoModel):
            pk: HashKey[str]


def test_multiple_hash_key_fields_raises():
    with pytest.raises(TypeError, match="multiple HashKey"):

        @table("t6")
        class M(DynamoModel):
            pk1: HashKey[str]
            pk2: HashKey[str]


def test_multiple_range_key_fields_raises():
    with pytest.raises(TypeError, match="multiple RangeKey"):

        @table("t7")
        class M(DynamoModel):
            pk: HashKey[str]
            sk1: RangeKey[str]
            sk2: RangeKey[str]


def test_no_hash_key_raises():
    with pytest.raises(TypeError, match="hash_key"):

        @table("t8")
        class M(DynamoModel):
            name: str
