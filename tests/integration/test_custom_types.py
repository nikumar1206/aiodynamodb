from datetime import datetime, timezone

from tests.integration.conftest import Metadata, TypedRecord


async def test_timestamp_roundtrip(db_typed):
    # datetime is serialised to a unix-seconds integer in DynamoDB and must
    # deserialise back to an equal datetime (Timestamp has second precision).
    dt = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    record = TypedRecord(
        record_id="r1",
        created_at=dt,
        metadata=Metadata(version=1, tags=["a"]),
    )
    await db_typed.put(record)

    fetched = await db_typed.get(TypedRecord, hash_key="r1")
    assert fetched is not None
    # Round-trip via unix seconds — sub-second precision is lost, so compare
    # at second granularity.
    assert int(fetched.created_at.timestamp()) == int(dt.timestamp())


async def test_timestamp_preserves_value_across_updates(db_typed):
    dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    await db_typed.put(TypedRecord(
        record_id="r1",
        created_at=dt,
        metadata=Metadata(version=1, tags=[]),
    ))

    # A second put with a different timestamp must overwrite correctly.
    dt2 = datetime(2025, 6, 15, 8, 30, 0, tzinfo=timezone.utc)
    await db_typed.put(TypedRecord(
        record_id="r1",
        created_at=dt2,
        metadata=Metadata(version=2, tags=["updated"]),
    ))

    fetched = await db_typed.get(TypedRecord, hash_key="r1")
    assert int(fetched.created_at.timestamp()) == int(dt2.timestamp())


async def test_json_str_roundtrip(db_typed):
    # Metadata is serialised to a JSON string in DynamoDB and must deserialise
    # back to the original Metadata instance.
    meta = Metadata(version=3, tags=["x", "y", "z"])
    await db_typed.put(TypedRecord(
        record_id="r1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        metadata=meta,
    ))

    fetched = await db_typed.get(TypedRecord, hash_key="r1")
    assert fetched is not None
    assert fetched.metadata.version == 3
    assert fetched.metadata.tags == ["x", "y", "z"]


async def test_json_str_empty_tags(db_typed):
    meta = Metadata(version=1, tags=[])
    await db_typed.put(TypedRecord(
        record_id="r1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        metadata=meta,
    ))

    fetched = await db_typed.get(TypedRecord, hash_key="r1")
    assert fetched.metadata.tags == []


async def test_custom_types_query(db_typed):
    # Verify that query returns correctly deserialised custom-type fields.
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(3):
        await db_typed.put(TypedRecord(
            record_id=f"r{i}",
            created_at=base,
            metadata=Metadata(version=i, tags=[f"tag{i}"]),
        ))

    from boto3.dynamodb.conditions import Attr

    items = [
        item
        async for page in db_typed.scan(TypedRecord, filter_expression=Attr("record_id").begins_with("r"))
        for item in page.items
    ]

    assert len(items) == 3
    for item in items:
        assert int(item.created_at.timestamp()) == int(base.timestamp())
        assert isinstance(item.metadata, Metadata)
        assert len(item.metadata.tags) == 1
