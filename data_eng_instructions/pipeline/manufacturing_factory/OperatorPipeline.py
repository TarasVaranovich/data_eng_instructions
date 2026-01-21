from typing import Any

from pandas.core.interchange.dataframe_protocol import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import IntegerType

from data_eng_instructions.constant.stubs import DEFAULT_ID
from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.line_factory.dwh.LineFactoryDefinitionDWH import LineFactoryDefinitionDWH
from data_eng_instructions.filedefinition.operatorsrooster.source.OperatorsRoosterDefinitionSource import \
    OperatorsRoosterDefinitionSource
from data_eng_instructions.filedefinition.shift.dwh.ShiftDefinitionDWH import ShiftDefinitionDWH
from data_eng_instructions.filedefinition.team.dwh.TeamDefinitionDWH import TeamDefinitionDWH
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.LineFactoryReader import LineFactoryReader
from data_eng_instructions.reader.OperatorsRoosterReader import OperatorsRoosterReader
from data_eng_instructions.reader.ShiftReder import ShiftReader
from data_eng_instructions.reader.TeamReader import TeamReader
from data_eng_instructions.schema.dwh.Operator import OPERATOR
from data_eng_instructions.transform.OperatosRoosterTransform import csv_to_type
from data_eng_instructions.utils.path_utility import storage_path
from data_eng_instructions.writer.OperatorWriter import OperatorWriter


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

        print("Read operators from source:")
        operator_df: DataFrame = reader.read_batch().transform(csv_to_type)

        print("Read teams:")
        team_definition: TeamDefinitionDWH = TeamDefinitionDWH(file_type)
        team_reader: TeamReader = TeamReader(spark, team_definition)
        team_df: DataFrame = team_reader.read_from_storage()

        print("Read shifts:")
        sh_definition: ShiftDefinitionDWH = ShiftDefinitionDWH(file_type)
        sh_reader: ShiftReader = ShiftReader(spark, sh_definition)
        sh_df: DataFrame = sh_reader.read_from_storage()

        print("Read line factories:")
        lf_definition: LineFactoryDefinitionDWH = LineFactoryDefinitionDWH(file_type)
        lf_reader: LineFactoryReader = LineFactoryReader(spark, lf_definition)
        lf_df: DataFrame = lf_reader.read_from_storage()

        operator_tm = (
            operator_df.alias("opr")
            .join(
                team_df.alias("tm"),
                F.col("opr.team") == F.col("tm.team_name"),
                how="left"
            )
            .withColumn(
                "team_id",
                F.coalesce(F.col("tm.team_id"), F.lit(DEFAULT_ID))
            )
            .drop("team", "team_name")
        )

        operator_sh = (
            operator_tm.alias("opr")
            .join(
                sh_df.alias("sh"),
                F.col("opr.primary_shift") == F.col("sh.shift_name"),
                how="left"
            )
            .withColumn(
                "primary_shift_id",
                F.coalesce(F.col("sh.shift_id"), F.lit(DEFAULT_ID))
            )
            .drop("primary_shift", "shift_name", "shift_id")
        )

        operator_lf = ((
                           operator_sh.alias("opr")
                           .join(
                               lf_df.alias("lf"),
                               (F.col("opr.primary_line") == F.col("lf.line_natural_key")) &
                               (F.col("opr.factory_id") == F.col("lf.factory_natural_key")),
                               how="left"
                           )
                           .withColumn(
                               "primary_line_factory_id",
                               F.coalesce(F.col("lf.line_factory_id"), F.lit(DEFAULT_ID))
                           )
                           .drop(
                               "line_natural_key",
                               "factory_natural_key",
                               "primary_line",
                               "factory_id",
                               "line_factory_id",
                               "certifications"
                           )
                       )
                       .withColumnRenamed("operator_id", "operator_natural_key")
                       .withColumn("operator_id", monotonically_increasing_id().cast(IntegerType())))

        operator_lf.show(5)
        result = spark.createDataFrame(
            operator_lf.rdd,
            OPERATOR
        )

        print("Write operators:")
        writer: OperatorWriter = OperatorWriter(result, storage_path())
        self.write_to_storage(writer, "operator")