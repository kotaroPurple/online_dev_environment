
from collections.abc import Iterator

from data import BaseTimeSeries
from dataset import Dataset


class StreamDataLoader:
    def __init__(
        self,
        dataset: Dataset,
        *,
        max_blocks: int | None = None,
        slice_size: int | None = None,
        drop_last: bool = False,
    ) -> None:
        if slice_size is not None and slice_size <= 0:
            raise ValueError("slice_size must be positive")

        self._dataset = dataset
        self._max_blocks = max_blocks
        self._slice_size = slice_size
        self._drop_last = drop_last

    def __iter__(self) -> Iterator[BaseTimeSeries]:
        emitted = 0
        for block in self._dataset:
            if self._slice_size is None:
                if self._max_blocks is not None and emitted >= self._max_blocks:
                    return
                yield block
                emitted += 1
                continue

            start = 0
            while start < block.block_size:
                if self._max_blocks is not None and emitted >= self._max_blocks:
                    return
                stop = start + self._slice_size
                window = block[start:stop]
                if window.block_size < self._slice_size and self._drop_last:
                    break
                yield window
                emitted += 1
                if window.block_size < self._slice_size:
                    break
                start += self._slice_size


class WindowedStreamDataLoader:
    def __init__(
        self,
        dataset: Dataset,
        *,
        window_size: int,
        hop_size: int | None = None,
        max_windows: int | None = None,
        drop_last: bool = False,
    ) -> None:
        if window_size <= 0:
            raise ValueError("window_size must be positive")
        if hop_size is not None and hop_size <= 0:
            raise ValueError("hop_size must be positive when provided")

        self._dataset = dataset
        self._window_size = window_size
        self._hop_size = hop_size or window_size
        self._max_windows = max_windows
        self._drop_last = drop_last

    def __iter__(self) -> Iterator[BaseTimeSeries]:
        emitted = 0
        for block in self._dataset:
            start = 0
            while start < block.block_size:
                if self._max_windows is not None and emitted >= self._max_windows:
                    return

                stop = start + self._window_size
                if stop > block.block_size and self._drop_last:
                    break

                window_block = block[start:stop]
                yield window_block

                emitted += 1
                start += self._hop_size
