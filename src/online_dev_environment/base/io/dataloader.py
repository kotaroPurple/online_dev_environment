"""Stream data loader for src_4th."""

from __future__ import annotations

from collections.abc import Iterator

from ..data.base_data import BaseTimeSeries
from .dataset import Dataset


class StreamDataLoader:
    def __init__(self, dataset: Dataset, *, max_blocks: int | None = None) -> None:
        self._dataset = dataset
        self._max_blocks = max_blocks

    def __iter__(self) -> Iterator[BaseTimeSeries]:
        count = 0
        for block in self._dataset:
            if self._max_blocks is not None and count >= self._max_blocks:
                return
            yield block
            count += 1
