from pyspark.sql import SparkSession

from data_eng_instructions.constant import relative_path
from data_eng_instructions.service import session
from tests.integration.read_file_from_resource import read_file_from_resource


def test_project_root():
    spark: SparkSession = session.make_csv_local()
    df = read_file_from_resource(spark, relative_path.MAINTENANCE_EVENTS)
    assert df.isEmpty() is False
    df.show(10)
    spark.stop()
