"""SQLAlchemy schema shared by the migration and PostgreSQL repository."""

from __future__ import annotations

from sqlalchemy import Column, Date, DateTime, Index, MetaData, Numeric, Table, Text, func

metadata = MetaData(schema="transactions")

transactions_table = Table(
    "transactions",
    metadata,
    Column("transaction_id", Text, primary_key=True),
    Column("user_id", Text, nullable=False),
    Column("original_amount", Numeric(24, 8), nullable=False),
    Column("currency", Text, nullable=False),
    Column("currency_allowlist_version", Text, nullable=False),
    Column("event_timestamp", DateTime(timezone=True), nullable=False),
    Column("rate_provider", Text, nullable=False),
    Column("rate_requested_date", Date, nullable=False),
    Column("rate_date", Date, nullable=False),
    Column("applied_rate", Numeric(24, 12), nullable=False),
    Column("converted_usd_amount", Numeric(24, 8), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

Index(
    "ix_transactions_user_id_event_timestamp_transaction_id",
    transactions_table.c.user_id,
    transactions_table.c.event_timestamp,
    transactions_table.c.transaction_id,
)
