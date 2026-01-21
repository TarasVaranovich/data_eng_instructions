from typing import Any

from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.order.dwh.OrderDefinitionDWH import OrderDefinitionDWH
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.OrderReader import OrderReader


class ManufacturingFactoryPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Running Defect Pipeline - Manufacturing Factory")
        print("Read orders:")
        file_type: FileType = self._param.get_result_type()
        spark: SparkSession = self._param.get_spark()
        order_definition: OrderDefinitionDWH = OrderDefinitionDWH(file_type)
        order_reader: OrderReader = OrderReader(spark, order_definition)
        order_reader.read_from_storage().show(10)
