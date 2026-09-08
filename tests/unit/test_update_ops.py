from datetime import datetime

import pytest
from pydantic_core import TzInfo

from aiodynamodb import DynamoModel, HashKey, UpdateAttr, table
from aiodynamodb.custom_types import Timestamp
from tests.unit.entities import Basket, ComplexOrder, Item, User, UserType, UserTypeT, UserVersion


@pytest.mark.parametrize("initial", [[], [Item(qty=1, price=10.9, name="foo")]])
async def test_update_appends_nested_list_and_combines_set(db, initial):
    created_at = datetime(2020, 1, 1, tzinfo=TzInfo())
    await db.put(ComplexOrder(order_id="o1", created_at=created_at, total=100, basket=Basket(items=initial)))
    added = [Item(qty=2, price=5.5, name="bar"), Item(qty=3, price=2.5, name="baz")]

    updated = await db.update(
        ComplexOrder,
        hash_key="o1",
        range_key=created_at,
        update_expression={UpdateAttr("basket.items").append(added), UpdateAttr("total").set(200)},
        return_values="ALL_NEW",
    )

    assert updated is not None
    assert updated.basket.items == initial + added
    assert updated.total == 200


@pytest.mark.parametrize("indexed_path", [False, True])
async def test_update_removes_list_element_by_index(db, indexed_path):
    items = [Item(qty=i, price=1, name=str(i)) for i in range(3)]
    created_at = datetime(2020, 1, 1, tzinfo=TzInfo())
    await db.put(ComplexOrder(order_id="o1", created_at=created_at, total=100, basket=Basket(items=items)))
    action = UpdateAttr("basket.items[1]").remove() if indexed_path else UpdateAttr("basket.items").remove(1)

    updated = await db.update(
        ComplexOrder,
        hash_key="o1",
        range_key=created_at,
        update_expression={action},
        return_values="ALL_NEW",
    )

    assert updated is not None
    assert updated.basket.items == [items[0], items[2]]


@pytest.mark.parametrize("method", ["add", "delete"])
def test_list_operands_rejected_for_set_actions(method):
    with pytest.raises(TypeError):
        getattr(UpdateAttr("items"), method)(["item"])


@pytest.mark.parametrize("value", ["item", {"item"}, None])
@pytest.mark.parametrize("method", ["append", "prepend"])
def test_append_requires_list(method, value):
    with pytest.raises(TypeError, match="requires a list"):
        getattr(UpdateAttr("items"), method)(value)


@pytest.mark.parametrize("index, error", [(-1, ValueError), (True, TypeError), (1.5, TypeError), ("1", TypeError)])
def test_remove_rejects_invalid_list_index(index, error):
    with pytest.raises(error):
        UpdateAttr("items").remove(index)


def test_remove_rejects_index_on_already_indexed_path():
    with pytest.raises(ValueError, match="already ends with a list index"):
        UpdateAttr("items[0]").remove(1)


@pytest.mark.parametrize("initial", [None, ["a"]])
@pytest.mark.parametrize("method, expected", [("append", ["a", "b", "c"]), ("prepend", ["b", "c", "a"])])
async def test_append_prepend_create_missing_list_by_default(db, initial, method, expected):
    @table("list_append_defaults")
    class Tagged(DynamoModel):
        item_id: HashKey[str]
        tags: list[str] | None = None

    await db.create_table(Tagged)
    await db.put(Tagged(item_id="i1", tags=initial))

    updated = await db.update(
        Tagged,
        hash_key="i1",
        update_expression={getattr(UpdateAttr("tags"), method)(["b", "c"])},
        return_values="ALL_NEW",
    )

    assert updated is not None
    assert updated.tags == (expected if initial else ["b", "c"])


async def test_append_if_not_exists_false_requires_existing_list(db):
    @table("list_append_strict")
    class Tagged(DynamoModel):
        item_id: HashKey[str]
        tags: list[str] | None = None

    await db.create_table(Tagged)
    await db.put(Tagged(item_id="i1"))

    ex = await db.exceptions()
    with pytest.raises(ex.ClientError):
        await db.update(
            Tagged,
            hash_key="i1",
            update_expression={UpdateAttr("tags").append(["a"], if_not_exists=False)},
        )


def test_append_and_prepend_expression_shapes():
    from aiodynamodb.updates import UpdateExpressionBuilder

    appended = UpdateExpressionBuilder(ComplexOrder).build_update_expression({UpdateAttr("basket.items").append([])})
    assert appended.update_expression == "SET #n0.#n1 = list_append(if_not_exists(#n0.#n1, :v1), :v0)"
    assert appended.expression_attribute_values == {":v0": [], ":v1": []}

    strict = UpdateExpressionBuilder(ComplexOrder).build_update_expression({
        UpdateAttr("basket.items").prepend([], if_not_exists=False)
    })
    assert strict.update_expression == "SET #n0.#n1 = list_append(:v0, #n0.#n1)"


async def test_append_omits_none_fields_like_put(db):
    from pydantic import BaseModel

    class Sub(BaseModel):
        a: int
        b: str | None = None

    @table("list_append_none")
    class Holder(DynamoModel):
        item_id: HashKey[str]
        subs: list[Sub] = []

    await db.create_table(Holder)
    await db.put(Holder(item_id="i1", subs=[Sub(a=1)]))
    await db.update(Holder, hash_key="i1", update_expression={UpdateAttr("subs").append([Sub(a=2)])})

    raw_table = await db._table("list_append_none")
    raw = (await raw_table.get_item(Key={"item_id": "i1"}))["Item"]
    assert raw["subs"] == [{"a": 1}, {"a": 2}]


@pytest.mark.parametrize("method", ["add", "delete"])
def test_frozenset_accepted_for_set_actions(method):
    attr = getattr(UpdateAttr("tags"), method)(frozenset({"a"}))
    assert attr.value == frozenset({"a"})


async def test_update_supports_high_level_update_expression(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="ALL_NEW",
    )

    assert updated == User(user_id="u1", name="Bob", email="alice@example.com")


async def test_update_serializes_timestamp_fields(db):
    @table("update_events")
    class Event(DynamoModel):
        event_id: HashKey[str]
        processed_at: Timestamp | None = None

    await db.create_table(Event)
    await db.put(Event(event_id="e1"))

    ts = datetime(2020, 1, 1, tzinfo=TzInfo())
    updated = await db.update(
        Event,
        hash_key="e1",
        update_expression={UpdateAttr("processed_at").set(ts)},
        return_values="ALL_NEW",
    )

    assert updated == Event(event_id="e1", processed_at=ts)


async def test_update_supports_nested_field_paths(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo")])
    created_at = datetime(2020, 1, 1, tzinfo=TzInfo())
    await db.put(ComplexOrder(order_id="o1", created_at=created_at, total=100, basket=basket))

    updated = await db.update(
        ComplexOrder,
        hash_key="o1",
        range_key=created_at,
        update_expression={UpdateAttr("basket.items[0].qty").set(7)},
        return_values="ALL_NEW",
    )

    assert updated is not None
    assert updated.basket.items[0].qty == 7


async def test_update_supports_atomic_counter_increment(db):
    @table("counter_values")
    class Counter(DynamoModel):
        counter_id: HashKey[str]
        value: int = 0

    await db.create_table(Counter)
    await db.put(Counter(counter_id="c1", value=0))

    first = await db.update(
        Counter,
        hash_key="c1",
        update_expression={UpdateAttr("value").add(2)},
        return_values="ALL_NEW",
    )
    second = await db.update(
        Counter,
        hash_key="c1",
        update_expression={UpdateAttr("value").add(3)},
        return_values="ALL_NEW",
    )

    assert first == Counter(counter_id="c1", value=2)
    assert second == Counter(counter_id="c1", value=5)


async def test_update_supports_specific_indexed_list_element(db):
    basket = Basket(items=[Item(qty=1, price=10.9, name="foo"), Item(qty=2, price=5.5, name="bar")])
    created_at = datetime(2020, 1, 1, tzinfo=TzInfo())
    await db.put(ComplexOrder(order_id="o1", created_at=created_at, total=100, basket=basket))

    updated = await db.update(
        ComplexOrder,
        hash_key="o1",
        range_key=created_at,
        update_expression={UpdateAttr("basket.items[1].qty").set(9)},
        return_values="ALL_NEW",
    )

    assert updated is not None
    assert updated.basket.items[0].qty == 1
    assert updated.basket.items[1].qty == 9


async def test_update_returns_model_instance(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
        return_values="ALL_NEW",
    )

    assert updated == User(user_id="u1", name="Bob", email="alice@example.com")


async def test_update_returns_none_without_return_values(db):
    await db.put(User(user_id="u1", name="Alice", email="alice@example.com"))

    updated = await db.update(
        User,
        hash_key="u1",
        update_expression={UpdateAttr("name").set("Bob")},
    )

    assert updated is None


async def test_update_supports_remove_add_and_delete_actions(db):
    @table("counter_users")
    class CounterUser(DynamoModel):
        user_id: HashKey[str]
        score: int = 0
        tags: set[str] | None = None
        email: str | None = None

    await db.create_table(CounterUser)
    await db.put(CounterUser(user_id="u1", score=1, email="alice@example.com"))

    updated = await db.update(
        CounterUser,
        hash_key="u1",
        update_expression={
            UpdateAttr("score").add(3),
            UpdateAttr("email").remove(),
        },
        return_values="ALL_NEW",
    )

    assert updated == CounterUser(user_id="u1", score=4, tags=None, email=None)

    # tags is absent (not NULL) — ADD on an absent set attribute creates it
    after_add = await db.update(
        CounterUser,
        hash_key="u1",
        update_expression={UpdateAttr("tags").add({"a", "b"})},
        return_values="ALL_NEW",
    )
    assert after_add.tags == {"a", "b"}

    # DELETE removes elements from an existing set
    after_delete = await db.update(
        CounterUser,
        hash_key="u1",
        update_expression={UpdateAttr("tags").delete({"b"})},
        return_values="ALL_NEW",
    )
    assert after_delete.tags == {"a"}


@pytest.mark.parametrize(
    "initial, added, expected",
    [
        pytest.param(None, {"a"}, {"a"}, id="create-single-member"),
        pytest.param(None, {"a", "b"}, {"a", "b"}, id="create-multiple-members"),
        pytest.param({"a"}, {"b"}, {"a", "b"}, id="add-single-member"),
        pytest.param({"a"}, {"b", "c"}, {"a", "b", "c"}, id="add-multiple-members"),
        pytest.param({"a", "b"}, {"b", "c"}, {"a", "b", "c"}, id="deduplicate-members"),
    ],
)
async def test_update_adds_set_members(db, initial, added, expected):
    @table("set_additions")
    class TaggedItem(DynamoModel):
        item_id: HashKey[str]
        tags: set[str] | None = None

    await db.create_table(TaggedItem)
    await db.put(TaggedItem(item_id="i1", tags=initial))

    updated = await db.update(
        TaggedItem,
        hash_key="i1",
        update_expression={UpdateAttr("tags").add(added)},
        return_values="ALL_NEW",
    )

    assert updated == TaggedItem(item_id="i1", tags=expected)
    assert await db.get(TaggedItem, hash_key="i1") == updated


@pytest.mark.parametrize(
    "removed, expected",
    [
        pytest.param({"b"}, {"a", "c"}, id="remove-single-member"),
        pytest.param({"a", "c"}, {"b"}, id="remove-multiple-members"),
        pytest.param({"missing"}, {"a", "b", "c"}, id="ignore-absent-member"),
        pytest.param({"b", "missing"}, {"a", "c"}, id="remove-existing-and-absent-members"),
        pytest.param({"a", "b", "c"}, None, id="remove-all-members"),
    ],
)
async def test_update_deletes_set_members(db, removed, expected):
    @table("set_deletions")
    class TaggedItem(DynamoModel):
        item_id: HashKey[str]
        tags: set[str] | None = None

    await db.create_table(TaggedItem)
    await db.put(TaggedItem(item_id="i1", tags={"a", "b", "c"}))

    updated = await db.update(
        TaggedItem,
        hash_key="i1",
        update_expression={UpdateAttr("tags").delete(removed)},
        return_values="ALL_NEW",
    )

    assert updated == TaggedItem(item_id="i1", tags=expected)
    assert await db.get(TaggedItem, hash_key="i1") == updated


async def test_update_supports_enum_hash_key(db):
    await db.put(UserType(user_type=UserTypeT.bar, name="Alice"))

    updated = await db.update(
        UserType,
        hash_key=UserTypeT.bar,
        update_expression={UpdateAttr("name").set("Alice Updated")},
        return_values="ALL_NEW",
    )

    assert updated == UserType(user_type=UserTypeT.bar, name="Alice Updated")


async def test_update_supports_enum_range_key(db):
    await db.put(UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob"))

    updated = await db.update(
        UserVersion,
        hash_key="u1",
        range_key=UserTypeT.foo,
        update_expression={UpdateAttr("name").set("Bob Updated")},
        return_values="ALL_NEW",
    )

    assert updated == UserVersion(user_id="u1", user_type=UserTypeT.foo, name="Bob Updated")


def test_update_rejects_list_traversal_without_index():
    from aiodynamodb.updates import UpdateExpressionBuilder

    with pytest.raises(ValueError, match="without an index"):
        UpdateExpressionBuilder(ComplexOrder).build_update_expression([UpdateAttr("basket.items.qty").set(1)])


@pytest.mark.parametrize(
    "first, second",
    [
        ("name", "name"),
        ("basket.items", "basket.items[0]"),
        ("basket.items[0].qty", "basket"),
    ],
)
def test_update_rejects_overlapping_paths(first, second):
    from aiodynamodb.updates import UpdateExpressionBuilder

    with pytest.raises(ValueError, match="overlapping paths"):
        UpdateExpressionBuilder(ComplexOrder).build_update_expression([
            UpdateAttr(first).set(1),
            UpdateAttr(second).set(2),
        ])


def test_update_allows_sibling_paths():
    from aiodynamodb.updates import UpdateExpressionBuilder

    built = UpdateExpressionBuilder(ComplexOrder).build_update_expression([
        UpdateAttr("basket.items[0].qty").set(1),
        UpdateAttr("basket.items[1].qty").set(2),
        UpdateAttr("total").add(1),
    ])
    assert built.update_expression == "SET #n0.#n1[0].#n2 = :v0, #n3.#n4[1].#n5 = :v1 ADD #n6 :v2"


def test_update_rejects_empty_and_actionless_expressions():
    from aiodynamodb.updates import UpdateExpressionBuilder

    with pytest.raises(ValueError, match="at least one"):
        UpdateExpressionBuilder(User).build_update_expression([])
    with pytest.raises(ValueError, match="has no action"):
        UpdateExpressionBuilder(User).build_update_expression([UpdateAttr("name")])


def test_update_expression_accepts_list_and_preserves_order():
    from aiodynamodb.updates import UpdateExpressionBuilder

    built = UpdateExpressionBuilder(User).build_update_expression([
        UpdateAttr("email").set("b@example.com"),
        UpdateAttr("name").set("Bob"),
    ])
    assert built.update_expression == "SET #n0 = :v0, #n1 = :v1"
    assert built.expression_attribute_names == {"#n0": "email", "#n1": "name"}


async def test_update_accepts_list_of_actions(db):
    await db.put(User(user_id="u1", name="Alice"))
    updated = await db.update(
        User,
        hash_key="u1",
        update_expression=[UpdateAttr("name").set("Bob"), UpdateAttr("email").set("bob@example.com")],
        return_values="ALL_NEW",
    )
    assert updated == User(user_id="u1", name="Bob", email="bob@example.com")


@pytest.mark.parametrize("existing, expected", [(None, "first@example.com"), ("keep@example.com", "keep@example.com")])
async def test_set_if_not_exists(db, existing, expected):
    await db.put(User(user_id="u1", name="Alice", email=existing))
    updated = await db.update(
        User,
        hash_key="u1",
        update_expression=[UpdateAttr("email").set("first@example.com", if_not_exists=True)],
        return_values="ALL_NEW",
    )
    assert updated is not None
    assert updated.email == expected


def test_set_if_not_exists_rejects_none():
    with pytest.raises(ValueError, match="use remove"):
        UpdateAttr("email").set(None, if_not_exists=True)
