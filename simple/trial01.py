
from datetime import datetime, timezone

import numpy as np

from data import BaseTimeSeries
from dataset import IterableDataset
from dataloader import StreamDataLoader
from pipeline import PipelineBuilder, Pipeline
from node import MovingAverageNode


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
    dataset = IterableDataset(source)

    # dataloader: iteration of dataset
    loader = StreamDataLoader(dataset)
    for one in loader:
        print(type(one), one.values.shape, one.metadata)

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
