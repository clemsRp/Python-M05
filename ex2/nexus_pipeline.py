#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol


class ProcessingPipeline(ABC):

    def __init__(self, pipeline_id):
        self.stages = list()

    def add_stage(self, stage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


class JSONAdapter:

    def __init__(self, pipeline_id) -> None:
        super().__init__(pipeline_id)

    def process(self, data) -> Any:
        pass


class CSVAdapter:

    def __init__(self, pipeline_id) -> None:
        super().__init__(pipeline_id)

    def process(self, data) -> Any:
        pass


class StreamAdapter:

    def __init__(self, pipeline_id) -> None:
        super().__init__(pipeline_id)

    def process(self, data) -> Any:
        pass


class ProcessingStage(Protocol):

	def __init__(self):
		pass


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
