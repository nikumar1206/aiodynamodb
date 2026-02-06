"""Pytest fixtures for aiodynamodb tests."""

from collections.abc import Generator
from typing import Any

import pytest

from aiodynamodb._state import _global_state


@pytest.fixture(autouse=True)
def reset_state() -> Generator[None, Any, None]:
    """Reset global state before and after each test."""
    _global_state.reset()
    yield
    _global_state.reset()
