import pytest
from boto3.dynamodb.conditions import Attr

from aiodynamodb import TransactConditionCheck, TransactDelete, TransactGet, TransactPut
from aiodynamodb import UpdateAttr
from aiodynamodb.models import TransactUpdate
from tests.integration.conftest import User


async def test_transact_write_puts_multiple_items(db):
    await db.transact_write([
        TransactPut(User(user_id="u1", name="Alice")),
        TransactPut(User(user_id="u2", name="Bob")),
    ])
    assert (await db.get(User, hash_key="u1")).name == "Alice"
    assert (await db.get(User, hash_key="u2")).name == "Bob"


async def test_transact_write_mixed_operations(db):
    await db.put(User(user_id="u1", name="Alice"))
    await db.transact_write([
        TransactPut(User(user_id="u2", name="Bob")),
        TransactDelete(User, hash_key="u1"),
    ])
    assert await db.get(User, hash_key="u1") is None
    assert await db.get(User, hash_key="u2") is not None


async def test_transact_write_rolls_back_all_on_failure(db):
    await db.put(User(user_id="u2", name="Existing"))
    ex = await db.exceptions()
    with pytest.raises(ex.TransactionCanceledException):
        await db.transact_write([
            TransactPut(User(user_id="u1", name="Alice")),
            TransactConditionCheck(
                User,
                hash_key="u2",
                condition_expression=Attr("user_id").not_exists(),
            ),
        ])
    # u1 must NOT have been written — whole transaction rolled back
    assert await db.get(User, hash_key="u1") is None


async def test_transact_cancellation_reasons_per_item(db):
    """CancellationReasons is indexed by operation position."""
    await db.put(User(user_id="u1", name="Alice"))
    ex = await db.exceptions()
    try:
        await db.transact_write([
            TransactPut(
                User(user_id="u1", name="Bob"),
                condition_expression=Attr("user_id").not_exists(),
            ),
            TransactPut(User(user_id="u2", name="Carol")),
        ])
        pytest.fail("expected TransactionCanceledException")
    except ex.TransactionCanceledException as e:
        reasons = e.response["CancellationReasons"]
        assert len(reasons) == 2
        assert reasons[0]["Code"] == "ConditionalCheckFailed"
        assert reasons[1]["Code"] == "None"


async def test_transact_write_condition_check_operation(db):
    await db.put(User(user_id="u1", name="Alice", age=30))
    # Condition check passes — transaction succeeds
    await db.transact_write([
        TransactPut(User(user_id="u2", name="Bob")),
        TransactConditionCheck(User, hash_key="u1", condition_expression=Attr("age").gte(18)),
    ])
    assert await db.get(User, hash_key="u2") is not None


async def test_transact_write_update_operation(db):
    await db.put(User(user_id="u1", name="Alice"))
    await db.transact_write([
        TransactUpdate(
            User,
            hash_key="u1",
            update_expression={UpdateAttr("name").set("Alice Updated")},
        ),
    ])
    assert (await db.get(User, hash_key="u1")).name == "Alice Updated"


async def test_transact_get_returns_in_request_order(db):
    await db.put(User(user_id="u1", name="Alice"))
    await db.put(User(user_id="u2", name="Bob"))
    results = await db.transact_get([
        TransactGet(User, hash_key="u2"),
        TransactGet(User, hash_key="u1"),
    ])
    assert results[0].name == "Bob"
    assert results[1].name == "Alice"


async def test_transact_get_missing_item_returns_none(db):
    await db.put(User(user_id="u1", name="Alice"))
    results = await db.transact_get([
        TransactGet(User, hash_key="u1"),
        TransactGet(User, hash_key="nonexistent"),
    ])
    assert results[0].name == "Alice"
    assert results[1] is None


async def test_transact_get_all_missing(db):
    results = await db.transact_get([
        TransactGet(User, hash_key="x"),
        TransactGet(User, hash_key="y"),
    ])
    assert results == [None, None]


async def test_transact_write_idempotency_token(db):
    await db.transact_write(
        [TransactPut(User(user_id="u1", name="Alice"))],
        client_request_token="idempotency-token-1",
    )
    # Retry with same token and different payload — DynamoDB replays the original
    await db.transact_write(
        [TransactPut(User(user_id="u1", name="Should Not Apply"))],
        client_request_token="idempotency-token-1",
    )
    assert (await db.get(User, hash_key="u1")).name == "Alice"
