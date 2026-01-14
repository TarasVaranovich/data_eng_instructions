from pyspark.sql import SparkSession

from data_eng_instructions.constant.relative_path import MAINTENANCE_EVENTS
from data_eng_instructions.service import session
from data_eng_instructions.service.read_file_from_resource import read_file_from_resource
from data_eng_instructions.utils.path_utility import entity_from_path

def test():
    table_name: str = entity_from_path(MAINTENANCE_EVENTS)
    spark: SparkSession = session.make_csv_local()
    (read_file_from_resource(spark, MAINTENANCE_EVENTS)
     .createOrReplaceTempView(table_name))
    df = spark.sql(f"""
        WITH maintenance_events AS (
            SELECT DISTINCT event_id FROM {table_name}
        )
        SELECT COUNT(*) AS events_count
        FROM maintenance_events
    """)
    result: int = df.collect()[0]["events_count"]
    assert result == 94
    spark.stop()