from aiodynamodb import table, DynamoModel
from aiodynamodb.models import GSI, LSI


@table("users", hash_key="user_id")
class User(DynamoModel):
    user_id: str
    name: str
    email: str | None = None


order_gsi = GSI(
    name="order_gsi",
    hash_key="order_id",
    range_key="total",
)

order_lsi = LSI(
    name="order_lsi",
    range_key="total",
)


@table("orders", hash_key="order_id", range_key="created_at", indexes=[order_gsi, order_lsi])
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int