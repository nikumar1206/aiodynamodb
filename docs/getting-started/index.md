# Installation

## Requirements

- Python **3.12 or later**

## Install

```bash
pip install aiodynamodb
```

For testing support (mocked AWS via aiomoto):

```bash
pip install aiodynamodb[testing]
```

With uv:

```bash
uv add aiodynamodb
uv add --optional testing aiodynamodb  # for test support
```

## AWS credentials

`aiodynamodb` uses `aioboto3` under the hood, which reads credentials the same way as the standard AWS SDK:

For local development, you can either configure a local profile (via localstack or similar) or use `mock_dynamodb()` from the testing module — no real AWS needed.

See the [Testing guide](../guides/testing.md) for the mocked setup.
