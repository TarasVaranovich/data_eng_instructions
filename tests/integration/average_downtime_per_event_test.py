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
    df = spark.sql(f"SELECT COALESCE(AVG(CAST(downtime_min AS DECIMAL(18,2))), 0.00) AS avg_downtime FROM {table_name}")
    result: float = df.collect()[0]["avg_downtime"]
    assert result == Decimal('65.744681')
    spark.stop()