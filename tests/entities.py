from aiodynamodb import table, DynamoModel


@table("users", hash_key="user_id")
class User(DynamoModel):
    user_id: str
    name: str
    email: str | None = None


@table("orders", hash_key="order_id", range_key="created_at")
class Order(DynamoModel):
    order_id: str
    created_at: str
    total: int
