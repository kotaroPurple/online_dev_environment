
from collections import deque
# from time import perf_counter
from typing import Dict, Iterable, Iterator, Sequence

from data import BaseTimeSeries, BlockBuffer
from dataloader import StreamDataLoader
# from .monitoring import BlockSummary, ErrorPolicy, PipelineMonitor
from node import ProcessingNode


class PipelineBuilder:
    def __init__(
        self,
        *,
        input_key: str = "input",
        output_keys: Sequence[str] | None = None,
    ) -> None:
        self._input_key = input_key
        self._output_keys = tuple(output_keys) if output_keys else None
        self._nodes: list[ProcessingNode] = []

    def add_node(self, node: ProcessingNode) -> "PipelineBuilder":
        self._nodes.append(node)
        return self

    def build(
        self,
        dataloader: StreamDataLoader,
        # *,
        # monitor: PipelineMonitor | None = None,
        # on_error: ErrorPolicy = ErrorPolicy.STOP,
    ) -> "Pipeline":
        order = resolve_order(self._nodes, available={self._input_key})
        return Pipeline(
            dataloader=dataloader,
            nodes=order,
            input_key=self._input_key,
            output_keys=self._output_keys,
            # monitor=monitor,
            # error_policy=on_error,
        )


def resolve_order(
    nodes: Sequence[ProcessingNode],
    *,
    available: Iterable[str],
) -> list[ProcessingNode]:
    available_keys = set(available)
    pending = deque(nodes)
    order: list[ProcessingNode] = []

    while pending:
        progressed = False
        for _ in range(len(pending)):
            node = pending.popleft()
            if set(node.requires()).issubset(available_keys):
                order.append(node)
                available_keys.update(node.produces())
                progressed = True
            else:
                pending.append(node)
        if not progressed:
            missing = sorted(
                {
                    dep
                    for node in pending
                    for dep in node.requires()
                    if dep not in available_keys
                }
            )
            raise ValueError(f"Unresolved dependencies: {missing}")

    return order


class Pipeline:
    def __init__(
        self,
        *,
        dataloader: StreamDataLoader,
        nodes: Sequence[ProcessingNode],
        input_key: str,
        output_keys: Sequence[str] | None,
        # monitor: PipelineMonitor | None,
        # error_policy: ErrorPolicy,
    ) -> None:
        self._dataloader = dataloader
        self._nodes = list(nodes)
        self._input_key = input_key
        self._output_keys = tuple(output_keys) if output_keys else None
        # self._monitor = monitor
        # self._error_policy = error_policy

    def run(self) -> Iterator[Dict[str, BaseTimeSeries]]:
        buffer = BlockBuffer()
        for node in self._nodes:
            node.reset()

        for index, block in enumerate(self._dataloader):
            _ = index
            # block_start = perf_counter()
            # if self._monitor:
            #     self._monitor.on_block_start(index)
            buffer.clear()
            buffer.set(self._input_key, block)
            produced: Dict[str, BaseTimeSeries] = {self._input_key: block}

            # try:
            for node in self._nodes:
                inputs: Dict[str, BaseTimeSeries] = {}
                missing = []
                for key in node.requires():
                    try:
                        inputs[key] = buffer.get(key)
                    except KeyError:
                        missing.append(key)
                if missing:
                    continue
                outputs = node.process(inputs)
                for key, value in outputs.items():
                    buffer.set(key, value)
                    produced[key] = value
            # except Exception as error:  # pragma: no cover - user node error
            #     # wrapped = PipelineExecutionError(index, node.name, error)
            #     if self._monitor:
            #         self._monitor.on_error(index, node.name, error)
            #         duration = perf_counter() - block_start
            #         self._monitor.on_block_end(
            #             BlockSummary(index, duration, outputs=None)
            #         )
            #     # CONTINUE: skip block
            #     continue

            # duration = perf_counter() - block_start
            # if self._monitor:
            #     self._monitor.on_block_end(
            #         BlockSummary(index, duration, produced)
            #     )

            if self._output_keys is None:
                yield dict(produced)
            else:
                yield {key: produced[key] for key in self._output_keys if key in produced}
