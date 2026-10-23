"""Recovery proof for a SIGKILL after PostgreSQL commit and before delivery ACK."""

from __future__ import annotations

import os

import pytest

from tests.recovery.support import (
    RecoveryEnvironment,
    assert_queues_empty,
    checkpoint,
    completed,
    kill_at_checkpoint,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("RABBITMQ_URL"), reason="RABBITMQ_URL is not configured"
)


def test_committed_delivery_is_redelivered_as_duplicate_after_sigkill(
    recovery_environment: RecoveryEnvironment,
) -> None:
    transaction_id = "recovery-commit-before-ack"
    recovery_environment.publish_event(transaction_id)
    blocked, ipc = recovery_environment.spawn(mode="normal", checkpoint="after_processing")
    checkpoint(ipc, "after_processing")

    assert recovery_environment.row_count() == 1
    kill_at_checkpoint(blocked, ipc)
    recovery_environment.wait_for(
        lambda: recovery_environment.queue_states()["transactions.work"]["ready"] == 1,
        "the unacknowledged original delivery to be requeued",
    )

    restarted, restart_ipc = recovery_environment.spawn(mode="normal", checkpoint="none")
    result = completed(restarted, restart_ipc)
    restart_ipc.close()
    assert result["outcomes"] == ["Duplicate"]
    assert result["envelopes"] == [
        {
            "transaction_id": transaction_id,
            "attempt": 0,
            "currency_allowlist_version": "currencies-v1",
        }
    ]
    assert recovery_environment.row_count() == 1
    assert_queues_empty(recovery_environment)
