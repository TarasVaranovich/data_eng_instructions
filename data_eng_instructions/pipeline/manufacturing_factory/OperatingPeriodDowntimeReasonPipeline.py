from typing import Any

from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam


class OperatingPeriodDownTimeReasonPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Running Operating Period Downtime Reason Pipeline - Dummy Implementation")
        self._param.get_spark().range(1).count()