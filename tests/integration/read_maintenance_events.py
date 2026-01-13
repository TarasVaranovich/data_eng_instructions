from pyspark.sql import SparkSession

from data_eng_instructions.constant import relative_path
from data_eng_instructions.service import service
from tests.integration.read_file_from_resource import read_file_from_resource


def test_project_root():
    spark: SparkSession = service.get_spark_session_csv()
    read_file_from_resource(relative_path.MAINTENANCE_EVENTS, spark, 10)
    spark.stop()
