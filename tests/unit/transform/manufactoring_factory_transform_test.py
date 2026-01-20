from pyspark.sql import SparkSession

from data_eng_instructions.transform.ManufacturingFactoryTransform import csv_to_type, filter_out_invalid_defects_definitions
from data_eng_instructions.schema.file.ManufacturingFactory import MANUFACTURING_FACTORY_CSV
from tests.stub.test_data import VALID_ROW, DEFECT_EXAMPLE


def test_transform_csv_to_type():
    spark = SparkSession.builder.appName("Testing CSV to Type transform").getOrCreate()


    df = spark.createDataFrame(VALID_ROW, MANUFACTURING_FACTORY_CSV)
    result = df.transform(csv_to_type)
    # assert result.schema == MANUFACTURING_FACTORY
    result.show()

    assert result.count() == len(VALID_ROW)


def test_filter_out_invalid_defects_definitions():
    spark = SparkSession.builder.appName("Testing Defects validation transform").getOrCreate()

    sample_data = VALID_ROW + DEFECT_EXAMPLE
    df = spark.createDataFrame(sample_data, MANUFACTURING_FACTORY_CSV)
    result = df.transform(filter_out_invalid_defects_definitions)
    assert result.schema == MANUFACTURING_FACTORY_CSV
    result.show()

    assert result.count() == len(VALID_ROW)