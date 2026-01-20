from pyspark.sql.types import StructType, StructField, IntegerType

OPERATING_PERIOD_DOWNTIME_REASON = StructType([
    StructField("operating_period_id", IntegerType(), False),
    StructField("downtime_reason_id", IntegerType(), False)
])

DOWNTIME_REASON = StructType([
    StructField("downtime_reason_id", IntegerType(), False),
    StructField("downtime_reason_name", IntegerType(), False)
])