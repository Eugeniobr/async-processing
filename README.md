# Async Processing

A specification-driven Python interview project for asynchronous transaction processing.

**Current stage:** Initial Python project setup is complete. Transaction processing,
broker topology, persistence, and HTTP behavior have not yet been implemented.

## Documents

- [Original specification v0.1](docs/specification-v0.1.md)
- [Project constitution](.specify/memory/constitution.md)
- [Spec Kit workflow and setup](docs/spec-kit-workflow.md)

The v0.1 document is preserved as the historical baseline. The constitution records
subsequent decisions and project principles.

## Development setup

This increment provides package structure, dependency metadata, configuration validation,
and quality tooling only. It does not start a service.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
cp .env.example .env
python -m ruff format --check .
python -m ruff check .
python -m pytest
```

`config/currencies-v1.json` is the reviewed ECB/Frankfurter currency allow-list
snapshot. Configuration requires `DATABASE_URL` and `RABBITMQ_URL`; `.env.example`
provides development values. Provider access, validation at ingestion, and all runtime
behavior are deferred to later tasks.
