"""Global state management for aiodynamodb."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import aioboto3
from aiobotocore.config import AioConfig

if TYPE_CHECKING:
    from types_aiobotocore_dynamodb.service_resource import DynamoDBServiceResource, Table

    from aiodynamodb.model.base import Model


@dataclass
class GlobalState:
    """Container for global aiodynamodb state."""

    # the aioboto3 session
    session: aioboto3.Session | None = None

    # default region for all operations
    region: str | None = None

    # default endpoint URL
    endpoint_url: str | None = None

    # default AioConfig
    aio_config: AioConfig | None = None

    # registered models (table_name -> model class)
    models: dict[str, type["Model"]] = field(default_factory=dict)

    # cached resource context manager (entered)
    _resource: "DynamoDBServiceResource | None" = None

    # cached table references (table_name -> Table)
    _tables: dict[str, "Table"] = field(default_factory=dict)

    # whether init() has been called
    initialized: bool = False

    def reset(self) -> None:
        """Reset all state. For testing purposes."""
        self.session = None
        self.region = None
        self.endpoint_url = None
        self.aio_config = None
        self.models.clear()
        self._resource = None
        self._tables.clear()
        self.initialized = False

    def get_client_kwargs(self, model_cls: type["Model"] | None = None) -> dict[str, Any]:
        """Get kwargs for creating a boto3 client/resource.

        Merges global defaults with model-specific overrides.
        """
        kwargs: dict[str, Any] = {}

        # Global defaults
        if self.region:
            kwargs["region_name"] = self.region
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        if self.aio_config:
            kwargs["config"] = self.aio_config

        # merge model overrides
        if model_cls is not None:
            config = model_cls.__table_config__
            if config.region:
                kwargs["region_name"] = config.region
            if config.endpoint_url:
                kwargs["endpoint_url"] = config.endpoint_url
            if config.aio_config:
                kwargs["config"] = config.aio_config
            kwargs.update(config.client_kwargs)

        return kwargs


_global_state = GlobalState()
