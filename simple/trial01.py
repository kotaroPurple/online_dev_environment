
from datetime import datetime, timezone

import numpy as np

from data import BaseTimeSeries
from dataset import IterableDataset
from dataloader import StreamDataLoader
from pipeline import PipelineBuilder, Pipeline
from node import MovingAverageNode


def generate_source(total_blocks: int = 10, block_size: int = 100) -> BaseTimeSeries:
    now = datetime.now(tz=timezone.utc)
    total_samples = total_blocks * block_size
    grid = np.linspace(0.0, 2 * np.pi, total_samples, endpoint=False)
    sensors = ["sin", "cos"]
    values = np.stack(
        [
            np.sin(grid),
            np.cos(grid / 2.0),
        ],
        axis=1,
    )

    return BaseTimeSeries(
        values=values,
        sample_rate=256.0,
        timestamp=now,
        metadata={"sensors": sensors, "block_size": block_size},
    )


def trial() -> None:
    # list [BaseTimeSeries]
    series = generate_source()
    dataset = IterableDataset(series)

    # dataloader: iteration of dataset
    loader = StreamDataLoader(dataset, slice_size=32)

    # build pipeline
    builder = PipelineBuilder(
        input_key="my_input",
        output_keys=[
            "my_output",
        ],
    )

    builder.add_node(MovingAverageNode("my_input", "my_output", window=5))

    pipeline: Pipeline = builder.build(loader)

    for outputs in pipeline.run():
        print(type(outputs), outputs['my_output'].values.shape)


def main() -> None:
    trial()


if __name__ == "__main__":
    main()
