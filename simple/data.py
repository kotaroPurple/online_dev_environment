
"""やりとり用のデータ, データ格納 Buffer"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

import numpy as np
import numpy.typing as npt

Array = npt.NDArray[np.number]


def _ensure_array(values: npt.ArrayLike) -> Array:
    array = np.asarray(values)
    if array.ndim == 0:
        raise ValueError("values must be at least 1-D")
    return array


@dataclass(slots=True, frozen=True)
class BaseTimeSeries:
    """Immutable block of time-series data with metadata."""

    values: Array
    sample_rate: float
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        array = _ensure_array(self.values)
        if array.shape[0] == 0:
            raise ValueError("values must contain at least one sample")
        object.__setattr__(self, "values", array)

        if self.sample_rate <= 0:
            raise ValueError("sample_rate must be positive")

        ts = self.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        object.__setattr__(self, "timestamp", ts)
        object.__setattr__(self, "metadata", dict(self.metadata))

    @property
    def block_size(self) -> int:
        return int(self.values.shape[0])

    @property
    def duration_seconds(self) -> float:
        return float(self.block_size / self.sample_rate)

    def copy_with(
        self,
        *,
        values: npt.ArrayLike | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "BaseTimeSeries":
        new_values = _ensure_array(values) if values is not None else self.values.copy()
        new_metadata = dict(metadata) if metadata is not None else dict(self.metadata)
        return BaseTimeSeries(
            values=new_values,
            sample_rate=self.sample_rate,
            timestamp=self.timestamp,
            metadata=new_metadata,
        )


class BlockBuffer:
    def __init__(self) -> None:
        self._store: dict[str, BaseTimeSeries] = {}

    def set(self, key: str, block: BaseTimeSeries) -> None:
        self._store[key] = block

    def get(self, key: str) -> BaseTimeSeries:
        if key not in self._store:
            raise KeyError(f"Block '{key}' not found.")
        return self._store[key]

    def clear(self) -> None:
        self._store.clear()

    def keys(self) -> Iterable[str]:
        return self._store.keys()

    def items(self) -> Iterator[tuple[str, BaseTimeSeries]]:
        return iter(self._store.items())
