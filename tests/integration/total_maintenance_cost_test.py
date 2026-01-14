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
    df = spark.sql(f"SELECT COALESCE(SUM(CAST(cost_eur AS DECIMAL(18,2))), 0.00) AS total_cost FROM {table_name}")
    total_cost: float = df.collect()[0]["total_cost"]
    assert total_cost == Decimal('169389.62')
    spark.stop()