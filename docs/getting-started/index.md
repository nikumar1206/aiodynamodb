# Installation

## Requirements

- Python **3.12 or later**
- AWS credentials configured in your environment

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

- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- `~/.aws/credentials` and `~/.aws/config`
- IAM instance/task roles in EC2/ECS/Lambda

For local development, you can either configure a local profile or use `mock_dynamodb()` from the testing module — no real AWS needed.

See the [Testing guide](../guides/testing.md) for the mocked setup.
