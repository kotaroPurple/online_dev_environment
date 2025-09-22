
from abc import ABC, abstractmethod
from typing import Iterator

from data import BaseTimeSeries


class Dataset(ABC):
    @abstractmethod
    def __iter__(self) -> Iterator[BaseTimeSeries]:
        ...

    def __len__(self) -> int:
        raise TypeError("Dataset len() is not available")


class IterableDataset(Dataset):
    def __init__(self, series: BaseTimeSeries) -> None:
        self._series = series

    def __iter__(self) -> Iterator[BaseTimeSeries]:
        yield self._series

    def __len__(self) -> int:
        return self._series.block_size

    def __getitem__(self, item: int | slice) -> BaseTimeSeries:
        if isinstance(item, slice):
            return self._series[item]

        index = self._normalise_index(item)
        return self._series[index:index + 1]

    @property
    def series(self) -> BaseTimeSeries:
        return self._series

    def _normalise_index(self, index: int) -> int:
        if index < 0:
            index += self._series.block_size
        if index < 0 or index >= self._series.block_size:
            raise IndexError("sample index out of range")
        return index


# class MultiSensorDataset(Dataset):
#     """Dataset that combines multiple sensor iterables into synchronized samples."""

#     def __init__(self, sensors: dict[str, Iterable[BaseTimeSeries]]) -> None:
#         self._sensors = sensors

#     def __iter__(self) -> Iterator[BaseTimeSeries]:
#         iterators = {key: iter(blocks) for key, blocks in self._sensors.items()}
#         while True:
#             sample: dict[str, BaseTimeSeries] = {}
#             for key, iterator in iterators.items():
#                 try:
#                     sample[key] = next(iterator)
#                 except StopIteration:
#                     return
#             first = next(iter(sample.values()))
#             yield BaseTimeSeries(
#                 values=first.values,
#                 sample_rate=first.sample_rate,
#                 timestamp=first.timestamp,
#                 metadata={"sensors": sample},
#             )
