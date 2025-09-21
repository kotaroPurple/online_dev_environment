
"""Block buffer for sharing base time-series objects."""

from __future__ import annotations

from typing import Dict, Iterable, Iterator

from .base_data import BaseTimeSeries


class BlockBuffer:
    """Store and retrieve blocks by key."""

    def __init__(self) -> None:
        self._store: Dict[str, BaseTimeSeries] = {}

    def set(self, key: str, block: BaseTimeSeries) -> None:
        self._store[key] = block

    def get(self, key: str) -> BaseTimeSeries:
        if key not in self._store:
            raise KeyError(f"Block '{key}' not found in buffer")
        return self._store[key]

    def clear(self) -> None:
        self._store.clear()

    def keys(self) -> Iterable[str]:
        return self._store.keys()

    def items(self) -> Iterator[tuple[str, BaseTimeSeries]]:
        return iter(self._store.items())
