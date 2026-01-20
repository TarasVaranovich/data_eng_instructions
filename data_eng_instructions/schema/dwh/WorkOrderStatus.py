from pyspark.sql.types import StructType, StructField, StringType, IntegerType

WORK_ORDER_STATUS = StructType([
    StructField("work_order_status_id", IntegerType(), False),
    StructField("work_order_status", StringType(), False)
])
