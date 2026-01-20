from pandas import DataFrame
from pyspark.sql import SparkSession

from data_eng_instructions.schema.file.ManufacturingFactory import MANUFACTURING_FACTORY_CSV
from data_eng_instructions.writer.ManufactoringFactoryWriter import ManufacturingFactoryWriter
from tests.stub.test_data import VALID_ROW

DIR: str = ""

def get_dataframe() -> DataFrame:
    spark = SparkSession.builder.appName("Testing ManufacturingFactoryWriter").getOrCreate()
    return spark.createDataFrame(VALID_ROW, MANUFACTURING_FACTORY_CSV)
# TODO: Note how to read data from the result dirs
def test_write_csv():
    df = get_dataframe()

    manufacturing_factory_writer: ManufacturingFactoryWriter = ManufacturingFactoryWriter(df, DIR)
    manufacturing_factory_writer.write_csv("manufacturing_factory_valid_row_csv.csv")

def test_write_parquet():
    df = get_dataframe()

    manufacturing_factory_writer: ManufacturingFactoryWriter = ManufacturingFactoryWriter(df, DIR)
    manufacturing_factory_writer.write_parquet("manufacturing_factory_valid_row_parquet.parquet")