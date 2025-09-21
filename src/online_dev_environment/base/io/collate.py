"""Collation utilities for src_4th."""

from __future__ import annotations

from typing import Callable

from ..data.base_data import BaseTimeSeries

CollateFn = Callable[[object], BaseTimeSeries]


def default_collate(sample: object) -> BaseTimeSeries:
    if isinstance(sample, BaseTimeSeries):
        return sample
    if isinstance(sample, dict):
        return BaseTimeSeries(**sample)
    raise TypeError(f"Unsupported sample type: {type(sample)!r}")
