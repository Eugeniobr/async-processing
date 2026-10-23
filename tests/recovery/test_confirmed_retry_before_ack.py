"""Recovery proof for a SIGKILL after retry publisher confirm and before original ACK."""

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


def test_confirmed_retry_and_redelivered_original_converge_after_sigkill(
    recovery_environment: RecoveryEnvironment,
) -> None:
    transaction_id = "recovery-confirmed-retry-before-ack"
    recovery_environment.publish_event(transaction_id)
    blocked, ipc = recovery_environment.spawn(mode="retry", checkpoint="after_retry_publish")
    checkpoint(ipc, "after_retry_publish")

    # The hook is invoked only after TransactionPublisher received its positive confirm.
    kill_at_checkpoint(blocked, ipc)
    recovery_environment.wait_for(
        lambda: recovery_environment.queue_states()["transactions.work"]["ready"] >= 1,
        "the original and confirmed retry deliveries to return to work",
    )

    restarted, restart_ipc = recovery_environment.spawn(
        mode="normal", checkpoint="none", max_messages=2
    )
    result = completed(restarted, restart_ipc)
    restart_ipc.close()
    assert sorted(result["outcomes"]) == ["Duplicate", "Processed"]
    assert sorted(result["envelopes"], key=lambda envelope: envelope["attempt"]) == [
        {
            "transaction_id": transaction_id,
            "attempt": 0,
            "currency_allowlist_version": "currencies-v1",
        },
        {
            "transaction_id": transaction_id,
            "attempt": 1,
            "currency_allowlist_version": "currencies-v1",
        },
    ]
    assert recovery_environment.row_count() == 1
    assert_queues_empty(recovery_environment)
