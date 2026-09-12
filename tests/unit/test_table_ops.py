from typing import Literal

import pytest
from boto3.dynamodb.conditions import Key
from pydantic import ValidationError, create_model

from aiodynamodb import DynamoModel, HashKey, RangeKey, table
from aiodynamodb.models import GSI, LSI
from tests.unit.entities import UserStatusT, UserTypeT


@pytest.mark.parametrize("key_name", ["pk", "sk"])
@pytest.mark.parametrize(
    ("annotation", "values", "attribute_type"),
    [
        (Literal["fixed"], ["fixed"], "S"),
        (Literal["foo", "bar"], ["foo", "bar"], "S"),
        (Literal[1, 2], [1, 2], "N"),
        (Literal[b"foo", b"bar"], [b"foo", b"bar"], "B"),
        (Literal[UserTypeT.foo, UserTypeT.bar], [UserTypeT.foo, UserTypeT.bar], "N"),
        (Literal[UserStatusT.active], [UserStatusT.active], "S"),
    ],
)
async def test_literal_keys_roundtrip(db, key_name, annotation, values, attribute_type):
    fields = {"pk": (str, ...), "sk": (str, ...)}
    fields[key_name] = (annotation, ...)
    model = table("literal_keys", hash_key="pk", range_key="sk")(
        create_model("LiteralKeys", __base__=DynamoModel, **fields)
    )
    response = await db.create_table(model)
    definitions = {
        entry["AttributeName"]: entry["AttributeType"] for entry in response["TableDescription"]["AttributeDefinitions"]
    }
    assert definitions[key_name] == attribute_type

    for value in values:
        keys = {"pk": "fixed", "sk": "fixed", key_name: value}
        item = model(**keys)
        await db.put(item)
        assert await db.get(model, hash_key=keys["pk"], range_key=keys["sk"]) == item
        results = [
            result
            async for page in db.query(
                model, key_condition_expression=Key("pk").eq(keys["pk"]) & Key("sk").eq(keys["sk"])
            )
            for result in page.items
        ]
        assert results == [item]
        await db.delete(model, hash_key=keys["pk"], range_key=keys["sk"])
        assert await db.get(model, hash_key=keys["pk"], range_key=keys["sk"]) is None


async def test_literal_key_annotations_and_indexes(db):
    @table(
        "literal_indexes",
        indexes=[GSI("by_kind", hash_key="kind", range_key="version"), LSI("by_version", range_key="version")],
    )
    class Item(DynamoModel):
        pk: HashKey[Literal["item"]] = "item"
        sk: RangeKey[Literal[1, 2]]
        kind: Literal["record"] = "record"
        version: Literal[1, 2] = 1

    response = await db.create_table(Item)
    definitions = {
        entry["AttributeName"]: entry["AttributeType"] for entry in response["TableDescription"]["AttributeDefinitions"]
    }
    assert definitions == {"pk": "S", "sk": "N", "kind": "S", "version": "N"}
    item = Item(sk=1)
    await db.put(item)
    assert await db.get(Item, hash_key="item", range_key=1) == item
    with pytest.raises(ValidationError):
        Item(sk=3)


@pytest.mark.parametrize("key_name", ["pk", "sk"])
@pytest.mark.parametrize(
    "annotation", [Literal[True], Literal[None], Literal["foo", 1], Literal[1, True], Literal["foo", None]]
)
async def test_create_table_rejects_unsupported_literal_keys(db, key_name, annotation):
    fields = {"pk": (str, ...), "sk": (str, ...)}
    fields[key_name] = (annotation, ...)
    model = table("bad_literal_keys", hash_key="pk", range_key="sk")(
        create_model("BadLiteralKeys", __base__=DynamoModel, **fields)
    )
    with pytest.raises(TypeError, match=f"Unsupported key type for field '{key_name}'"):
        await db.create_table(model)


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
