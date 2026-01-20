from pyspark.sql import SparkSession

from data_eng_instructions.schema.dwh.Operator import TEAM
from data_eng_instructions.schema.file.OperatorsRooster import OPERATORS_ROOSTER_CSV
from data_eng_instructions.transform.KVExtractor import KVExtractor
from tests.stub.OperatorsRoosterData import VALID_ROW


def test_extract_team_from_operators_rooster():
    spark = SparkSession.builder.appName("Testing CSV to Type transform").getOrCreate()
    team_extractor: KVExtractor = (
        KVExtractor(
            OPERATORS_ROOSTER_CSV,
            TEAM,
            "team_id",
            "team",
            "team_name"
        )
    )
    df = spark.createDataFrame(VALID_ROW, OPERATORS_ROOSTER_CSV)
    result = df.transform(team_extractor.extract)
    assert result.schema == TEAM
    result.show()

    assert result.count() == len(VALID_ROW)