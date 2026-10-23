"""Create the transaction persistence schema.

This small migration is invoked with an explicit SQLAlchemy connection by local
verification code; it keeps the schema independent from a migration framework.
"""

from __future__ import annotations

from sqlalchemy import Connection
from sqlalchemy.schema import CreateSchema, DropSchema

from infrastructure.postgres_schema import metadata

revision = "001_transactions"
down_revision = None


def upgrade(connection: Connection) -> None:
    connection.execute(CreateSchema("transactions", if_not_exists=True))
    metadata.create_all(connection)


def downgrade(connection: Connection) -> None:
    metadata.drop_all(connection)
    connection.execute(DropSchema("transactions", cascade=True, if_exists=True))
