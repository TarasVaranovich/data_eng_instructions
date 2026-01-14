from pyspark.sql import SparkSession

from data_eng_instructions.constant import relative_path
from data_eng_instructions.service import session
from data_eng_instructions.service.read_file_from_resource import read_file_from_resource

def test():
    spark: SparkSession = session.make_csv_local()
    df = read_file_from_resource(spark, relative_path.MANUFACTURING_FACTORY_DATASET)
    assert df.isEmpty() is False
    df.printSchema()
    spark.stop()