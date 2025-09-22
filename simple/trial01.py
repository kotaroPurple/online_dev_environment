
from datetime import datetime, timezone

import numpy as np

from data import BaseTimeSeries
from dataloader import StreamDataLoader
from pipeline import PipelineBuilder, Pipeline


def generate_source(n_blocks: int = 10, block_size: int = 100) -> list[BaseTimeSeries]:
    now = datetime.now(tz=timezone.utc)
    grid = np.linspace(0.0, 2 * np.pi, block_size, endpoint=False)
    return [
        BaseTimeSeries(
            values=np.sin(grid + idx * np.pi / 32)[:, None],  # (N, 1)
            sample_rate=256.0,
            timestamp=now,
            metadata={"block_index": idx},
        )
        for idx in range(n_blocks)
    ]


def trial() -> None:
    # list [BaseTimeSeries]
    source = generate_source()

    # dataloader: iteration of dataset
    loader = StreamDataLoader(source)
    for one in loader:
        print(type(one))

    # build pipeline
    builder = PipelineBuilder(
        input_key="my_input",
        output_keys=[
            "my_output",
        ],
    )

    builder.add_node(
        DecisionNode(
            required_keys=["my_input"],
            output_key="my_output",
        )
    )

    pass

# def build_pipeline() -> None:
#     sensors = multisensor_source()
#     dataset = MultiSensorDataset(sensors)
#     loader = StreamDataLoader(dataset)

#     builder = PipelineBuilder(
#         input_key="multi",
#         output_keys=[
#             "sensor_a_window",
#             "sensor_b_window",
#             "decision",
#         ],
#     )
#     builder.add_node(SplitSensorNode("multi", ["sensor_a", "sensor_b"]))
#     builder.add_node(NormalizerNode("sensor_a_raw", "sensor_a_norm"))
#     builder.add_node(
#         SlidingWindowNode(
#             "sensor_a_norm",
#             "sensor_a_window",
#             window_seconds=5.0,
#             hop_seconds=1.0,
#         )
#     )
#     builder.add_node(NormalizerNode("sensor_b_raw", "sensor_b_norm"))
#     builder.add_node(
#         SlidingWindowNode(
#             "sensor_b_norm",
#             "sensor_b_window",
#             window_seconds=5.0,
#             hop_seconds=1.0,
#         )
#     )
#     builder.add_node(
#         DecisionNode(
#             required_keys=["sensor_a_window", "sensor_b_window"],
#             output_key="decision",
#         )
#     )

#     monitor = ConsoleMonitor(prefix="[quickstart]")
#     pipeline = builder.build(loader, monitor=monitor)

#     emitted = 0
#     for outputs in pipeline.run():
#         decision = outputs.get("decision")
#         if decision is None:
#             continue
#         score = decision.metadata.get("decision_score")
#         print(
#             "Decision score: {:.3f} (keys={})".format(
#                 float(score),
#                 list(outputs.keys()),
#             )
#         )
#         emitted += 1
#         if emitted >= 5:
#             break


def main() -> None:
    trial()


if __name__ == "__main__":
    main()
