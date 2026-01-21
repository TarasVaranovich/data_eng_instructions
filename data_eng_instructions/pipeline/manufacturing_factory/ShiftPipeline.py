from typing import Any

from pandas.core.interchange.dataframe_protocol import DataFrame

from data_eng_instructions.filedefinition.manufctoringfactory.source.ManufacturingFactoryDefinitionSource import \
    ManufacturingFactoryDefinitionSource
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.ManufacturingFactoryReader import ManufacturingFactoryReader
from data_eng_instructions.schema.dwh.Shift import SHIFT
from data_eng_instructions.schema.file.ManufacturingFactory import MANUFACTURING_FACTORY
from data_eng_instructions.transform.KVExtractor import KVExtractor
from data_eng_instructions.transform.ManufacturingFactoryTransform import csv_to_type
from data_eng_instructions.utils.path_utility import storage_path
from data_eng_instructions.writer.ShiftWriter import ShiftWriter


class ShiftPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    # TODO: more safer and reliable way is 'shifts' data
    # TODO: extraction from two sources: 'manufacturing_factory' and 'operators_rooster'
    # TODO: for now implementation is simplified because of actual data intersection
    # TODO: between two sources is empty
    def run(self) -> Any:
        print("Extract shifts")
        spark = self._param.get_spark()

        manufacturing_factory_definition: ManufacturingFactoryDefinitionSource = \
            (ManufacturingFactoryDefinitionSource())

        reader: ManufacturingFactoryReader = \
            ManufacturingFactoryReader(spark, manufacturing_factory_definition)

        extractor: KVExtractor = (
            KVExtractor(
                MANUFACTURING_FACTORY,
                SHIFT,
                "shift_id",
                "shift",
                "shift_name"
            )
        )

        result_df: DataFrame = ((reader.read_batch()
                                 .transform(csv_to_type)
                                 .transform(extractor.extract)))

        print("Write shifts")
        writer: ShiftWriter = ShiftWriter(result_df, storage_path())
        self.write_to_storage(writer, "shift")
