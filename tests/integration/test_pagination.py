"""
Pagination edge cases.

Key behaviour under test:
  - `Limit` caps items *evaluated*, not items *returned*. With a filter,
    a page can have zero matching items but still carry a LastEvaluatedKey.
  - Our async generator must keep fetching until there is no more cursor,
    even through empty intermediate pages.
  - Manual cursor round-trips must reconstruct the exact full dataset with
    no duplicates or gaps.
"""

from boto3.dynamodb.conditions import Attr, Key

from tests.integration.conftest import Order, User


async def test_scan_collects_all_items_across_pages(db):
    for i in range(20):
        await db.put(User(user_id=f"u{i:02d}", name=f"User{i}"))

    items = [item async for page in db.scan(User, limit=5) for item in page.items]

    assert len(items) == 20
    assert {u.user_id for u in items} == {f"u{i:02d}" for i in range(20)}


async def test_query_collects_all_items_across_pages(db):
    for i in range(10):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i * 10))

    items = [
        item
        async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1"), limit=3)
        for item in page.items
    ]

    assert len(items) == 10


async def test_filter_with_limit_returns_all_matching_items(db):
    """
    Limit=1 with a filter that rejects most items forces the generator
    through multiple pages, some of which may contain zero results.
    The generator must keep fetching until the cursor is exhausted.
    """
    # Only u4 matches the filter; u0-u3 do not
    for i in range(5):
        await db.put(User(user_id=f"u{i}", name=f"User{i}", active=(i == 4)))

    items = [
        item async for page in db.scan(User, limit=1, filter_expression=Attr("active").eq(True)) for item in page.items
    ]

    assert len(items) == 1
    assert items[0].user_id == "u4"


async def test_manual_cursor_no_duplicates_no_gaps(db):
    """Step through pages one at a time using exclusive_start_key."""
    for i in range(9):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i))

    collected = []
    cursor = None

    while True:
        page = None
        async for p in db.query(
            Order,
            key_condition_expression=Key("order_id").eq("o1"),
            limit=3,
            scan_index_forward=True,
            exclusive_start_key=cursor,
        ):
            page = p
            break  # take one page

        collected.extend(page.items)
        cursor = page.last_evaluated_key
        if cursor is None:
            break

    assert len(collected) == 9
    dates = [i.created_at for i in collected]
    # no duplicates
    assert len(dates) == len(set(dates))
    # ascending order preserved across page boundaries
    assert dates == sorted(dates)


async def test_last_page_has_no_cursor(db):
    for i in range(3):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i))

    pages = [page async for page in db.query(Order, key_condition_expression=Key("order_id").eq("o1"), limit=10)]

    assert len(pages) == 1
    assert pages[0].last_evaluated_key is None


async def test_cursor_resumes_exactly_where_left_off(db):
    for i in range(6):
        await db.put(Order(order_id="o1", created_at=f"2026-01-{i + 1:02d}", total=i))

    # First 3
    first_page = None
    async for p in db.query(Order, key_condition_expression=Key("order_id").eq("o1"), limit=3, scan_index_forward=True):
        first_page = p
        break

    assert len(first_page.items) == 3
    assert first_page.last_evaluated_key is not None

    # Resume from cursor
    second_items = []
    async for page in db.query(
        Order,
        key_condition_expression=Key("order_id").eq("o1"),
        limit=3,
        scan_index_forward=True,
        exclusive_start_key=first_page.last_evaluated_key,
    ):
        second_items.extend(page.items)

    assert len(second_items) == 3
    first_dates = {i.created_at for i in first_page.items}
    second_dates = {i.created_at for i in second_items}
    assert first_dates.isdisjoint(second_dates)
    assert first_dates | second_dates == {f"2026-01-{i + 1:02d}" for i in range(6)}


async def test_scan_limit_per_page_respected(db):
    for i in range(10):
        await db.put(User(user_id=f"u{i:02d}", name=f"User{i}"))

    pages = [page async for page in db.scan(User, limit=3)]

    # Each page must have at most `limit` items
    assert all(len(p.items) <= 3 for p in pages)
    # All items recovered across pages
    assert sum(len(p.items) for p in pages) == 10
