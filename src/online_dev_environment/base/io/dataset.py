"""Dataset implementations for src_4th."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Iterator

from ..data.base_data import BaseTimeSeries


class Dataset(ABC):
    @abstractmethod
    def __iter__(self) -> Iterator[BaseTimeSeries]:  # pragma: no cover - interface
        ...

    def __len__(self) -> int:
        raise TypeError("Dataset length not available")


class IterableDataset(Dataset):
    def __init__(self, blocks: Iterable[BaseTimeSeries]) -> None:
        self._blocks: list[BaseTimeSeries] = list(blocks)

    def __iter__(self) -> Iterator[BaseTimeSeries]:
        for block in self._blocks:
            yield block

    def __len__(self) -> int:
        return len(self._blocks)


class MultiSensorDataset(Dataset):
    """Dataset that combines multiple sensor iterables into synchronized samples."""

    def __init__(self, sensors: dict[str, Iterable[BaseTimeSeries]]) -> None:
        self._sensors = sensors

    def __iter__(self) -> Iterator[BaseTimeSeries]:
        iterators = {key: iter(blocks) for key, blocks in self._sensors.items()}
        while True:
            sample: dict[str, BaseTimeSeries] = {}
            for key, iterator in iterators.items():
                try:
                    sample[key] = next(iterator)
                except StopIteration:
                    return
            first = next(iter(sample.values()))
            yield BaseTimeSeries(
                values=first.values,
                sample_rate=first.sample_rate,
                timestamp=first.timestamp,
                metadata={"sensors": sample},
            )
