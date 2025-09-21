"""Adapter dataset that wraps arbitrary sources."""

from __future__ import annotations

from typing import Callable, Iterable, Iterator

from ..data.base_data import BaseTimeSeries
from .collate import CollateFn, default_collate
from .dataset import Dataset


class AdapterDataset(Dataset):
    """Wrap a callable or iterable and collate samples into BaseTimeSeries."""

    def __init__(
        self,
        source: Callable[[], Iterable[object]] | Iterable[object],
        *,
        collate_fn: CollateFn = default_collate,
    ) -> None:
        self._source = source
        self._collate_fn = collate_fn

    def __iter__(self) -> Iterator[BaseTimeSeries]:
        iterable = self._source() if callable(self._source) else self._source
        for sample in iterable:
            yield self._collate_fn(sample)
