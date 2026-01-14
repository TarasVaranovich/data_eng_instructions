from decimal import Decimal

from pyspark.sql import SparkSession

from data_eng_instructions.constant.relative_path import MAINTENANCE_EVENTS
from data_eng_instructions.service import session
from tests.integration.read_file_from_resource import read_file_from_resource
from data_eng_instructions.utils.path_utility import entity_from_path

def test():
    table_name: str = entity_from_path(MAINTENANCE_EVENTS)
    spark: SparkSession = session.make_csv_local()
    (read_file_from_resource(spark, MAINTENANCE_EVENTS)
     .createOrReplaceTempView(table_name))
    df = spark.sql(f"SELECT COALESCE(SUM(downtime_min)) AS downtime_min FROM {table_name}")
    total_cost: int = df.collect()[0]["downtime_min"]
    assert total_cost == 6180
    spark.stop()