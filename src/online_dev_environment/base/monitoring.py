"""Monitoring primitives for src_4th."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict

from .data import BaseTimeSeries


class ErrorPolicy(str, Enum):
    STOP = "stop"
    CONTINUE = "continue"


@dataclass(slots=True)
class BlockSummary:
    block_index: int
    duration_seconds: float
    outputs: Dict[str, BaseTimeSeries] | None


class PipelineMonitor:
    def on_block_start(self, block_index: int) -> None:  # pragma: no cover
        ...

    def on_block_end(self, summary: BlockSummary) -> None:  # pragma: no cover
        ...

    def on_error(
        self,
        block_index: int,
        node_name: str,
        error: Exception,
    ) -> None:  # pragma: no cover
        ...


class ConsoleMonitor(PipelineMonitor):
    def __init__(self, prefix: str = "[src_4th]") -> None:
        self._prefix = prefix

    def on_block_start(self, block_index: int) -> None:
        print(f"{self._prefix} block {block_index} start")

    def on_block_end(self, summary: BlockSummary) -> None:
        outputs = [] if summary.outputs is None else list(summary.outputs.keys())
        print(
            "{prefix} block {index} end duration={duration:.4f}s outputs={outputs}".format(
                prefix=self._prefix,
                index=summary.block_index,
                duration=summary.duration_seconds,
                outputs=outputs,
            )
        )

    def on_error(self, block_index: int, node_name: str, error: Exception) -> None:
        print(f"{self._prefix} block {block_index} error in {node_name}: {error!r}")
