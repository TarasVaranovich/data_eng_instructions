from typing import Any

from data_eng_instructions.transform.KVExtractor import KVExtractor
from data_eng_instructions.transform.ManufacturingFactoryTransform import csv_to_type

from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.manufctoringfactory.source.ManufacturingFactoryDefinitionSource import \
    ManufacturingFactoryDefinitionSource
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.ManufacturingFactoryReader import ManufacturingFactoryReader
from data_eng_instructions.schema.file.ManufacturingFactory import MANUFACTURING_FACTORY
from data_eng_instructions.schema.dwh.Order import ORDER


class OrderPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Extract orders")
        spark = self._param.get_spark()
        file_type: FileType = self._param.get_result_type()

        manufacturing_factory_definition: ManufacturingFactoryDefinitionSource = \
            (ManufacturingFactoryDefinitionSource())

        reader: ManufacturingFactoryReader = \
            ManufacturingFactoryReader(spark, manufacturing_factory_definition)

        team_extractor: KVExtractor = (
            KVExtractor(
                MANUFACTURING_FACTORY,
                ORDER,
                "order_id",
                "order_id",
                "order_natural_key"
            )
        )
        ((reader.read_batch()
         .transform(csv_to_type)
         .transform(team_extractor.extract)))
