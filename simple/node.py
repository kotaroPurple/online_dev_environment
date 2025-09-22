
from datetime import datetime, timedelta
from typing import Any, Iterable

import numpy as np

from data import BaseTimeSeries


class ProcessingNode:
    def __init__(self, name: str | None = None) -> None:
        self.name = name or self.__class__.__name__

    def requires(self) -> Iterable[str]:
        return []

    def produces(self) -> Iterable[str]:
        return []

    def reset(self) -> None:
        return

    def process(self, inputs: dict[str, BaseTimeSeries]) -> dict[str, BaseTimeSeries]:
        _ = inputs
        raise NotImplementedError


class MovingAverageNode(ProcessingNode):
    def __init__(self, key_in: str, key_out: str|None, *, window: int = 5) -> None:
        if window <= 0:
            raise ValueError("window must be positive")
        super().__init__()
        self._key_in = key_in
        self._key_out = key_out or f"{key_in}_ma{window}"
        self._window = window
        self._kernel = np.ones(window, dtype=np.float64) / window

    def requires(self) -> Iterable[str]:
        return [self._key_in]

    def produces(self) -> Iterable[str]:
        return [self._key_out]

    def process(self, inputs: dict[str, BaseTimeSeries]) -> dict[str, BaseTimeSeries]:
        block = inputs[self._key_in]
        if block.values.shape[0]< self._window:
            return {self._key_out: block}
        convolved = np.apply_along_axis(
            lambda m: np.convolve(m, self._kernel, mode='valid'), axis=0, arr=block.values)
        pad = block.values.shape[0] - convolved.shape[0]
        if pad > 0:
            prefix = np.repeat(convolved[0:1], pad, axis=0)
            smoothed = np.vstack([prefix, convolved])
        else:
            smoothed = convolved
        return {self._key_out: block.copy_with(values=smoothed)}


class WindowBufferNode(ProcessingNode):
    def __init__(
        self,
        key_in: str,
        key_out: str | None = None,
        *,
        window_size: int,
        hop_size: int | None = None,
    ) -> None:
        if window_size <= 0:
            raise ValueError("window_size must be positive")
        if hop_size is not None and hop_size <= 0:
            raise ValueError("hop_size must be positive when provided")
        super().__init__()
        self._key_in = key_in
        self._key_out = key_out or f"{key_in}_window"
        self._window_size = window_size
        self._hop_size = hop_size or window_size
        self._buffer: np.ndarray | None = None
        self._initial_timestamp: datetime | None = None
        self._sample_rate: float | None = None
        self._base_metadata: dict[str, Any] | None = None
        self._offset_samples = 0

    def requires(self) -> Iterable[str]:
        return [self._key_in]

    def produces(self) -> Iterable[str]:
        return [self._key_out]

    def reset(self) -> None:
        self._buffer = None
        self._initial_timestamp = None
        self._sample_rate = None
        self._base_metadata = None
        self._offset_samples = 0

    def process(self, inputs: dict[str, BaseTimeSeries]) -> dict[str, BaseTimeSeries]:
        block = inputs[self._key_in]
        if self._sample_rate is None:
            self._sample_rate = block.sample_rate
            self._initial_timestamp = block.timestamp
            self._base_metadata = dict(block.metadata)
        elif not np.isclose(self._sample_rate, block.sample_rate):
            raise ValueError("Sample rate mismatch in WindowBufferNode")

        self._buffer = block.values.copy() if self._buffer is None else np.vstack([self._buffer, block.values])

        if self._buffer.shape[0] < self._window_size or self._sample_rate is None or self._initial_timestamp is None:
            return {}

        window_start_index = self._offset_samples
        window_values = self._buffer[: self._window_size]

        metadata: dict[str, Any] = dict(self._base_metadata or {})
        metadata.update(block.metadata)
        metadata["window_size"] = self._window_size
        metadata["hop_size"] = self._hop_size
        metadata["slice"] = (window_start_index, window_start_index + self._window_size, 1)

        window_series = BaseTimeSeries(
            values=window_values,
            sample_rate=self._sample_rate,
            timestamp=self._initial_timestamp + timedelta(seconds=window_start_index / self._sample_rate),
            metadata=metadata,
        )

        hop = min(self._hop_size, self._buffer.shape[0])
        self._buffer = self._buffer[hop:]
        self._offset_samples += hop
        if self._buffer.shape[0] == 0:
            self._buffer = None

        return {self._key_out: window_series}


class FFTNode(ProcessingNode):
    def __init__(self, key_in: str, key_out: str | None = None) -> None:
        super().__init__()
        self._key_in = key_in
        self._key_out = key_out or f"{key_in}_fft"

    def requires(self) -> Iterable[str]:
        return [self._key_in]

    def produces(self) -> Iterable[str]:
        return [self._key_out]

    def process(self, inputs: dict[str, BaseTimeSeries]) -> dict[str, BaseTimeSeries]:
        block = inputs[self._key_in]
        fft_complex = np.fft.rfft(block.values, axis=0)
        magnitude = np.abs(fft_complex)
        frequencies = np.fft.rfftfreq(block.block_size, d=1.0 / block.sample_rate)

        metadata = dict(block.metadata)
        metadata["frequencies_hz"] = frequencies
        metadata["fft_magnitude"] = True

        spectrum = BaseTimeSeries(
            values=magnitude,
            sample_rate=block.sample_rate,
            timestamp=block.timestamp,
            metadata=metadata,
        )
        return {self._key_out: spectrum}


# class NormalizerNode(ProcessingNode):
#     def __init__(self, key_in: str, key_out: str | None = None, *, eps: float = 1e-9) -> None:
#         super().__init__()
#         self._key_in = key_in
#         self._key_out = key_out or f"{key_in}_norm"
#         self._eps = eps

#     def requires(self) -> Iterable[str]:
#         return [self._key_in]

#     def produces(self) -> Iterable[str]:
#         return [self._key_out]

#     def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
#         block = inputs[self._key_in]
#         peak = np.max(np.abs(block.values))
#         if peak < self._eps:
#             return {self._key_out: block}
#         scaled = block.values / peak
#         metadata = {**block.metadata, "scale": float(1.0 / peak)}
#         return {self._key_out: block.copy_with(values=scaled, metadata=metadata)}


# class SlidingWindowNode(ProcessingNode):
#     """Accumulate samples until a time window is full, then emit with hop."""

#     def __init__(
#         self,
#         key_in: str,
#         key_out: str,
#         *,
#         window_seconds: float,
#         hop_seconds: float,
#     ) -> None:
#         super().__init__()
#         if window_seconds <= 0 or hop_seconds <= 0:
#             raise ValueError("window_seconds and hop_seconds must be positive")
#         self._key_in = key_in
#         self._key_out = key_out
#         self._window_seconds = window_seconds
#         self._hop_seconds = hop_seconds
#         self._buffer: list[np.ndarray] = []
#         self._sample_rate: float | None = None
#         self._window_samples: int | None = None
#         self._hop_samples: int | None = None

#     def requires(self) -> Iterable[str]:
#         return [self._key_in]

#     def produces(self) -> Iterable[str]:
#         return [self._key_out]

#     def reset(self) -> None:
#         self._buffer.clear()
#         self._sample_rate = None
#         self._window_samples = None
#         self._hop_samples = None

#     def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
#         block = inputs[self._key_in]
#         if self._sample_rate is None:
#             self._sample_rate = block.sample_rate
#             self._window_samples = max(int(round(self._window_seconds * self._sample_rate)), 1)
#             self._hop_samples = max(int(round(self._hop_seconds * self._sample_rate)), 1)
#         elif not np.isclose(self._sample_rate, block.sample_rate):
#             raise ValueError("Sample rate changed during SlidingWindowNode processing")

#         self._buffer.append(block.values)
#         concatenated = np.concatenate(self._buffer, axis=0)

#         if self._window_samples is None or concatenated.shape[0] < self._window_samples:
#             return {}

#         window_vals = concatenated[: self._window_samples]
#         window_block = block.copy_with(
#             values=window_vals,
#             metadata={**block.metadata, "window_seconds": self._window_seconds},
#         )

#         # Trim buffer by hop
#         trim = self._hop_samples or 0
#         if trim >= concatenated.shape[0]:
#             self._buffer.clear()
#         else:
#             remaining = concatenated[trim:]
#             self._buffer = [remaining]

#         return {self._key_out: window_block}


# class SplitSensorNode(ProcessingNode):
#     """Split a multi-sensor dict into individual keys."""

#     def __init__(self, input_key: str, sensor_keys: Iterable[str]) -> None:
#         super().__init__()
#         self._input_key = input_key
#         self._sensor_keys = list(sensor_keys)

#     def requires(self) -> Iterable[str]:
#         return [self._input_key]

#     def produces(self) -> Iterable[str]:
#         return [f"{sensor}_raw" for sensor in self._sensor_keys]

#     def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
#         block = inputs[self._input_key]
#         # metadata must include per-sensor blocks to split
#         sensors = block.metadata.get("sensors")
#         if sensors is None:
#             raise ValueError("SplitSensorNode requires metadata['sensors'] with per-sensor blocks")
#         outputs: Dict[str, BaseTimeSeries] = {}
#         for sensor in self._sensor_keys:
#             sensor_block = sensors.get(sensor)
#             if sensor_block is None:
#                 raise ValueError(f"Sensor '{sensor}' not found in metadata")
#             outputs[f"{sensor}_raw"] = sensor_block
#         return outputs


# class DecisionNode(ProcessingNode):
#     """Combine sensor features and emit a decision block."""

#     def __init__(self, required_keys: Iterable[str], output_key: str = "decision") -> None:
#         super().__init__()
#         self._required_keys = list(required_keys)
#         self._output_key = output_key

#     def requires(self) -> Iterable[str]:
#         return self._required_keys

#     def produces(self) -> Iterable[str]:
#         return [self._output_key]

#     def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
#         score = sum(np.mean(block.values) for block in inputs.values()) / len(inputs)
#         first = next(iter(inputs.values()))
#         decision_block = first.copy_with(
#             values=np.array([[score]], dtype=np.float64),
#             metadata={"decision_score": float(score)},
#         )
#         return {self._output_key: decision_block}
