from pyspark.sql import SparkSession

from data_eng_instructions.schema.file.OperatorsRooster import OPERATORS_ROOSTER_CSV, OPERATORS_ROOSTER
from data_eng_instructions.transform.OperatosRoosterTransform import csv_to_type
from tests.stub.OperatorsRoosterData import VALID_ROW


def test_transform_csv_to_type():
    spark = SparkSession.builder.appName("Testing CSV to Type transform").getOrCreate()

    df = spark.createDataFrame(VALID_ROW, OPERATORS_ROOSTER_CSV)
    result = df.transform(csv_to_type)
    #assert result.schema == OPERATORS_ROOSTER
    result.show()

    assert result.count() == len(VALID_ROW)