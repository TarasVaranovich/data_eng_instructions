from typing import Any

from pandas.core.interchange.dataframe_protocol import DataFrame

from data_eng_instructions.filedefinition.manufctoringfactory.source.ManufacturingFactoryDefinitionSource import \
    ManufacturingFactoryDefinitionSource
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.ManufacturingFactoryReader import ManufacturingFactoryReader
from data_eng_instructions.schema.dwh.Order import ORDER
from data_eng_instructions.schema.file.ManufacturingFactory import MANUFACTURING_FACTORY
from data_eng_instructions.transform.KVExtractor import KVExtractor
from data_eng_instructions.transform.ManufacturingFactoryTransform import csv_to_type
from data_eng_instructions.writer.OrderWriter import OrderWriter


class OrderPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Extract orders")
        spark = self._param.get_spark()

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
        order_df: DataFrame = ((reader.read_batch()
                                .transform(csv_to_type)
                                .transform(team_extractor.extract)))

        print("Write orders")
        order_writer: OrderWriter = OrderWriter(order_df, Pipeline.storage_path())
        self.write_to_storage(order_writer, "order")
