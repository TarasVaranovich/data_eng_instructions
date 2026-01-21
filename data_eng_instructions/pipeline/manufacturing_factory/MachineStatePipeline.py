from typing import Any

from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam


class MachineStatePipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Running Defect Pipeline - Machine State")
        self._param.get_spark().range(1).count()
