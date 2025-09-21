"""Fourth-stage pipeline prototype approaching production architecture."""

from .data.base_data import BaseTimeSeries
from .data.buffer import BlockBuffer
from .io import (
    AdapterDataset,
    CollateFn,
    IterableDataset,
    StreamDataLoader,
    MultiSensorDataset
)
from .monitoring import ConsoleMonitor, ErrorPolicy, PipelineMonitor
from .nodes import (
    DecisionNode,
    MovingAverageNode,
    NormalizerNode,
    SlidingWindowNode,
    SplitSensorNode,
)
from .pipeline import PipelineBuilder, PipelineExecutionError, PipelineOrchestrator

__all__ = [
    "BaseTimeSeries",
    "BlockBuffer",
    "AdapterDataset",
    "CollateFn",
    "IterableDataset",
    "StreamDataLoader",
    "MultiSensorDataset",
    "ConsoleMonitor",
    "ErrorPolicy",
    "PipelineMonitor",
    "DecisionNode",
    "MovingAverageNode",
    "NormalizerNode",
    "SlidingWindowNode",
    "SplitSensorNode",
    "PipelineBuilder",
    "PipelineExecutionError",
    "PipelineOrchestrator",
]
