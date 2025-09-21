
"""Quickstart for src_4th showing sliding window and smoothing."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np

from online_dev_environment.base import BaseTimeSeries
from online_dev_environment.base import MultiSensorDataset, StreamDataLoader
from online_dev_environment.base import ConsoleMonitor
from online_dev_environment.base import DecisionNode, NormalizerNode, SlidingWindowNode, SplitSensorNode
from online_dev_environment.base import PipelineBuilder


def multisensor_source(
    num_blocks: int = 120,
    *,
    block_size: int = 256,
) -> dict[str, list[BaseTimeSeries]]:
    now = datetime.now(tz=timezone.utc)
    sensors = {
        "sensor_a": [],
        "sensor_b": [],
    }
    grid = np.linspace(0.0, 2 * np.pi, block_size, endpoint=False)
    for idx in range(num_blocks):
        for sid, phase_shift in [("sensor_a", 0.0), ("sensor_b", np.pi / 4)]:
            values = np.sin(grid + idx * np.pi / 32 + phase_shift)[:, None]
            sensors[sid].append(
                BaseTimeSeries(
                    values=values,
                    sample_rate=256.0,
                    timestamp=now,
                    metadata={"sensor": sid, "block_index": idx},
                )
            )
    return sensors


def build_pipeline() -> None:
    sensors = multisensor_source()
    dataset = MultiSensorDataset(sensors)
    loader = StreamDataLoader(dataset)

    builder = PipelineBuilder(
        input_key="multi",
        output_keys=[
            "sensor_a_window",
            "sensor_b_window",
            "decision",
        ],
    )
    builder.add_node(SplitSensorNode("multi", ["sensor_a", "sensor_b"]))
    builder.add_node(NormalizerNode("sensor_a_raw", "sensor_a_norm"))
    builder.add_node(
        SlidingWindowNode(
            "sensor_a_norm",
            "sensor_a_window",
            window_seconds=5.0,
            hop_seconds=1.0,
        )
    )
    builder.add_node(NormalizerNode("sensor_b_raw", "sensor_b_norm"))
    builder.add_node(
        SlidingWindowNode(
            "sensor_b_norm",
            "sensor_b_window",
            window_seconds=5.0,
            hop_seconds=1.0,
        )
    )
    builder.add_node(
        DecisionNode(
            required_keys=["sensor_a_window", "sensor_b_window"],
            output_key="decision",
        )
    )

    monitor = ConsoleMonitor(prefix="[quickstart]")
    pipeline = builder.build(loader, monitor=monitor)

    emitted = 0
    for outputs in pipeline.run():
        decision = outputs.get("decision")
        if decision is None:
            continue
        score = decision.metadata.get("decision_score")
        print(
            "Decision score: {:.3f} (keys={})".format(
                float(score),
                list(outputs.keys()),
            )
        )
        emitted += 1
        if emitted >= 5:
            break


def main() -> None:
    build_pipeline()


if __name__ == "__main__":  # pragma: no cover
    main()
