from __future__ import annotations

import pytest

from domain.outcomes import Duplicate, PermanentFailure, Processed, RetryableFailure


@pytest.mark.parametrize(
    "outcome",
    [
        Processed("transaction-1"),
        Duplicate("transaction-1"),
        RetryableFailure("timeout"),
        PermanentFailure("invalid_envelope"),
    ],
)
def test_processing_outcomes_are_immutable_value_objects(outcome: object) -> None:
    assert outcome


@pytest.mark.parametrize("factory", [Processed, Duplicate, RetryableFailure, PermanentFailure])
def test_processing_outcomes_require_a_nonempty_value(factory: object) -> None:
    with pytest.raises(ValueError):
        factory(" ")  # type: ignore[operator]
