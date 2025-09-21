"""Processing nodes for src_4th."""

from __future__ import annotations

from typing import Dict, Iterable

import numpy as np

from .data import BaseTimeSeries


class ProcessingNode:
    def __init__(self, name: str | None = None) -> None:
        self.name = name or self.__class__.__name__

    def requires(self) -> Iterable[str]:
        return []

    def produces(self) -> Iterable[str]:
        return []

    def reset(self) -> None:
        return

    def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
        raise NotImplementedError


class NormalizerNode(ProcessingNode):
    def __init__(self, key_in: str, key_out: str | None = None, *, eps: float = 1e-9) -> None:
        super().__init__()
        self._key_in = key_in
        self._key_out = key_out or f"{key_in}_norm"
        self._eps = eps

    def requires(self) -> Iterable[str]:
        return [self._key_in]

    def produces(self) -> Iterable[str]:
        return [self._key_out]

    def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
        block = inputs[self._key_in]
        peak = np.max(np.abs(block.values))
        if peak < self._eps:
            return {self._key_out: block}
        scaled = block.values / peak
        metadata = {**block.metadata, "scale": float(1.0 / peak)}
        return {self._key_out: block.copy_with(values=scaled, metadata=metadata)}


class MovingAverageNode(ProcessingNode):
    def __init__(self, key_in: str, key_out: str | None = None, *, window: int = 5) -> None:
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

    def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
        block = inputs[self._key_in]
        if block.values.shape[0] < self._window:
            return {self._key_out: block}
        convolved = np.apply_along_axis(
            lambda m: np.convolve(m, self._kernel, mode="valid"),
            axis=0,
            arr=block.values,
        )
        pad = block.values.shape[0] - convolved.shape[0]
        if pad > 0:
            prefix = np.repeat(convolved[0:1], pad, axis=0)
            smoothed = np.vstack([prefix, convolved])
        else:
            smoothed = convolved
        return {self._key_out: block.copy_with(values=smoothed)}


class SlidingWindowNode(ProcessingNode):
    """Accumulate samples until a time window is full, then emit with hop."""

    def __init__(
        self,
        key_in: str,
        key_out: str,
        *,
        window_seconds: float,
        hop_seconds: float,
    ) -> None:
        super().__init__()
        if window_seconds <= 0 or hop_seconds <= 0:
            raise ValueError("window_seconds and hop_seconds must be positive")
        self._key_in = key_in
        self._key_out = key_out
        self._window_seconds = window_seconds
        self._hop_seconds = hop_seconds
        self._buffer: list[np.ndarray] = []
        self._sample_rate: float | None = None
        self._window_samples: int | None = None
        self._hop_samples: int | None = None

    def requires(self) -> Iterable[str]:
        return [self._key_in]

    def produces(self) -> Iterable[str]:
        return [self._key_out]

    def reset(self) -> None:
        self._buffer.clear()
        self._sample_rate = None
        self._window_samples = None
        self._hop_samples = None

    def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
        block = inputs[self._key_in]
        if self._sample_rate is None:
            self._sample_rate = block.sample_rate
            self._window_samples = max(int(round(self._window_seconds * self._sample_rate)), 1)
            self._hop_samples = max(int(round(self._hop_seconds * self._sample_rate)), 1)
        elif not np.isclose(self._sample_rate, block.sample_rate):
            raise ValueError("Sample rate changed during SlidingWindowNode processing")

        self._buffer.append(block.values)
        concatenated = np.concatenate(self._buffer, axis=0)

        if self._window_samples is None or concatenated.shape[0] < self._window_samples:
            return {}

        window_vals = concatenated[: self._window_samples]
        window_block = block.copy_with(
            values=window_vals,
            metadata={**block.metadata, "window_seconds": self._window_seconds},
        )

        # Trim buffer by hop
        trim = self._hop_samples or 0
        if trim >= concatenated.shape[0]:
            self._buffer.clear()
        else:
            remaining = concatenated[trim:]
            self._buffer = [remaining]

        return {self._key_out: window_block}


class SplitSensorNode(ProcessingNode):
    """Split a multi-sensor dict into individual keys."""

    def __init__(self, input_key: str, sensor_keys: Iterable[str]) -> None:
        super().__init__()
        self._input_key = input_key
        self._sensor_keys = list(sensor_keys)

    def requires(self) -> Iterable[str]:
        return [self._input_key]

    def produces(self) -> Iterable[str]:
        return [f"{sensor}_raw" for sensor in self._sensor_keys]

    def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
        block = inputs[self._input_key]
        # metadata must include per-sensor blocks to split
        sensors = block.metadata.get("sensors")
        if sensors is None:
            raise ValueError("SplitSensorNode requires metadata['sensors'] with per-sensor blocks")
        outputs: Dict[str, BaseTimeSeries] = {}
        for sensor in self._sensor_keys:
            sensor_block = sensors.get(sensor)
            if sensor_block is None:
                raise ValueError(f"Sensor '{sensor}' not found in metadata")
            outputs[f"{sensor}_raw"] = sensor_block
        return outputs


class DecisionNode(ProcessingNode):
    """Combine sensor features and emit a decision block."""

    def __init__(self, required_keys: Iterable[str], output_key: str = "decision") -> None:
        super().__init__()
        self._required_keys = list(required_keys)
        self._output_key = output_key

    def requires(self) -> Iterable[str]:
        return self._required_keys

    def produces(self) -> Iterable[str]:
        return [self._output_key]

    def process(self, inputs: Dict[str, BaseTimeSeries]) -> Dict[str, BaseTimeSeries]:
        score = sum(np.mean(block.values) for block in inputs.values()) / len(inputs)
        first = next(iter(inputs.values()))
        decision_block = first.copy_with(
            values=np.array([[score]], dtype=np.float64),
            metadata={"decision_score": float(score)},
        )
        return {self._output_key: decision_block}
