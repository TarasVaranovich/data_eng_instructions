from typing import Any

from pandas.core.interchange.dataframe_protocol import DataFrame

from data_eng_instructions.filedefinition.manufctoringfactory.source.ManufacturingFactoryDefinitionSource import \
    ManufacturingFactoryDefinitionSource
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.ManufacturingFactoryReader import ManufacturingFactoryReader
from data_eng_instructions.transform.LineFactoryTransform import extract_line_factory
from data_eng_instructions.transform.ManufacturingFactoryTransform import csv_to_type
from data_eng_instructions.utils.path_utility import storage_path
from data_eng_instructions.writer.LineFactoryWriter import LineFactoryWriter


class LineFactoryPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Extract Line Factory data")
        spark = self._param.get_spark()

        manufacturing_factory_definition: ManufacturingFactoryDefinitionSource = \
            (ManufacturingFactoryDefinitionSource())

        reader: ManufacturingFactoryReader = \
            ManufacturingFactoryReader(spark, manufacturing_factory_definition)

        result_df: DataFrame = ((reader.read_batch()
                                 .transform(csv_to_type)
                                 .transform(extract_line_factory)))

        print("Write line factories")
        writer: LineFactoryWriter = LineFactoryWriter(result_df, storage_path())
        self.write_to_storage(writer, "line_factory")
