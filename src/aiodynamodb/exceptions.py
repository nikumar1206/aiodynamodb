"""Exception hierarchy for aiodynamodb."""

from pydantic import ValidationError as PydanticValidationError
from pydantic_core import ErrorDetails


class AioDynamoDBError(Exception):
    """Base exception for all aiodynamodb errors."""


class NotInitializedError(AioDynamoDBError):
    """Raised when operations attempted before init() called."""


class ItemNotFoundError(AioDynamoDBError):
    """Raised when get() doesn't find an item."""


class TableNotFoundError(AioDynamoDBError):
    """Raised when table doesn't exist."""


class ConditionalCheckFailedError(AioDynamoDBError):
    """Raised when a conditional write fails."""


class TransactionCancelledError(AioDynamoDBError):
    """Raised when a transaction is cancelled."""


class ValidationError(AioDynamoDBError):
    """Wraps Pydantic validation errors.

    Catchable as AioDynamoDBError, but also exposes the original
    Pydantic error for detailed error inspection.
    """

    def __init__(self, pydantic_error: PydanticValidationError) -> None:
        self.pydantic_error = pydantic_error
        super().__init__(str(pydantic_error))

    @property
    def errors(self) -> list[ErrorDetails]:
        """Proxy to pydantic_error.errors() for convenience."""
        return self.pydantic_error.errors()
