"""Pytest suite for BaseTimeSeries using simple synthetic sensor data."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pytest

from online_dev_environment.base.data import BaseTimeSeries


def _make_block() -> BaseTimeSeries:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    values = np.array([[0.0], [1.0], [2.0]], dtype=np.float64)
    return BaseTimeSeries(
        values=values,
        sample_rate=100.0,
        timestamp=now,
        metadata={"sensor": "accelerometer"},
    )


def test_from_list_constructs_block() -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    values = [[0.1], [0.2], [0.3], [0.4]]

    block = BaseTimeSeries(
        values=values,
        sample_rate=50.0,
        timestamp=now,
        metadata={"sensor": "gyroscope"},
    )

    assert block.block_size == 4
    assert pytest.approx(block.duration_seconds) == 0.08
    assert block.metadata["sensor"] == "gyroscope"
    assert block.values.shape == (4, 1)
    assert isinstance(block.values, np.ndarray)


def test_naive_timestamp_promoted_to_utc() -> None:
    naive = datetime(2024, 1, 1, 12, 0, 0)

    block = BaseTimeSeries(
        values=[[0.0], [0.1]],
        sample_rate=10.0,
        timestamp=naive,
        metadata={},
    )

    assert block.timestamp.tzinfo == timezone.utc


def test_copy_with_overrides_values_and_metadata() -> None:
    block = _make_block()
    new_values = np.ones_like(block.values) * 5.0

    copied = block.copy_with(
        values=new_values,
        metadata={"sensor": "accelerometer", "gain": 2.0},
    )

    np.testing.assert_allclose(copied.values, new_values)
    assert copied.metadata["gain"] == 2.0
    assert copied.values is not block.values
    assert copied.metadata is not block.metadata


def test_invalid_values_raise() -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError):
        BaseTimeSeries(
            values=1.23,
            sample_rate=100.0,
            timestamp=now,
            metadata={},
        )
