"""Minimal BaseTimeSeries usage example with synthetic sensor data."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np

from online_dev_environment.base.data import BaseTimeSeries


def main() -> None:
    now = datetime.now(tz=timezone.utc)
    sample_rate = 100.0
    seconds = 1.0
    samples = int(sample_rate * seconds)

    # Simulate a single-axis accelerometer
    # time_axis = np.linspace(0.0, seconds, samples, endpoint=False)
    accel_values = 0.1 * np.random.randn(samples, 1) + 9.81  # noisy gravity vector

    block = BaseTimeSeries(
        values=accel_values,
        sample_rate=sample_rate,
        timestamp=now,
        metadata={"sensor": "accelerometer", "unit": "m/s^2"},
    )

    print(f"Block size: {block.block_size} samples")
    print(f"Duration: {block.duration_seconds:.3f} seconds")
    print(f"First sample: {block.values[0, 0]:.3f} {block.metadata['unit']}")
    print(f"All samples: {block.values[:, 0]}, {block.values.shape}")


if __name__ == "__main__":  # pragma: no cover
    main()
