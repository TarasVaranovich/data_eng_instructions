from abc import ABC, abstractmethod
from typing import Any

from data_eng_instructions.pipeline.PipelineParam import PipelineParam


class Pipeline(ABC):
    def __init__(self, param: PipelineParam):
        self._param = param

    @abstractmethod
    def run(self) -> Any:
        pass
