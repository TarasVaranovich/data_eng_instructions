from typing import Any

from pandas.core.interchange.dataframe_protocol import DataFrame

from data_eng_instructions.filedefinition.operatorsrooster.source.OperatorsRoosterDefinitionSource import \
    OperatorsRoosterDefinitionSource
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.OperatorsRoosterReader import OperatorsRoosterReader
from data_eng_instructions.schema.dwh.Operator import TEAM
from data_eng_instructions.schema.file.OperatorsRooster import OPERATORS_ROOSTER
from data_eng_instructions.transform.KVExtractor import KVExtractor
from data_eng_instructions.transform.OperatosRoosterTransform import csv_to_type
from data_eng_instructions.utils.path_utility import storage_path
from data_eng_instructions.writer.TeamWriter import TeamWriter


class TeamPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Extract teams")
        spark = self._param.get_spark()

        operators_rooster_definition: OperatorsRoosterDefinitionSource = \
            (OperatorsRoosterDefinitionSource())

        reader: OperatorsRoosterReader = \
            OperatorsRoosterReader(spark, operators_rooster_definition)

        extractor: KVExtractor = (
            KVExtractor(
                OPERATORS_ROOSTER,
                TEAM,
                "team_id",
                "team",
                "team_name"
            )
        )
        result_df: DataFrame = ((reader.read_batch()
                                 .transform(csv_to_type)
                                 .transform(extractor.extract)))

        print("Write teams")
        writer: TeamWriter = TeamWriter(result_df, storage_path())
        self.write_to_storage(writer, "team")
