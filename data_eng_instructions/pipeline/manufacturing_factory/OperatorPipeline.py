from typing import Any

from pandas.core.interchange.dataframe_protocol import DataFrame
from pyspark.sql import SparkSession

from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.operatorsrooster.source.OperatorsRoosterDefinitionSource import \
    OperatorsRoosterDefinitionSource
from data_eng_instructions.filedefinition.team.dwh.TeamDefinitionDWH import TeamDefinitionDWH
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.OperatorsRoosterReader import OperatorsRoosterReader
from data_eng_instructions.reader.TeamReader import TeamReader
from data_eng_instructions.transform.OperatosRoosterTransform import csv_to_type


class OperatorPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Extract operators")
        file_type: FileType = self._param.get_result_type()
        spark: SparkSession = self._param.get_spark()

        operators_rooster_definition: OperatorsRoosterDefinitionSource = \
            (OperatorsRoosterDefinitionSource())

        reader: OperatorsRoosterReader = \
            OperatorsRoosterReader(spark, operators_rooster_definition)

        operator_df: DataFrame = reader.read_batch().transform(csv_to_type)
        team_definition: TeamDefinitionDWH = TeamDefinitionDWH(file_type)
        team_reader: TeamReader = TeamReader(spark, team_definition)
        team_df: DataFrame = team_reader.read_from_storage()
        team_df.show(5)

        # TODO: shift
