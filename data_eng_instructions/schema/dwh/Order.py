from pyspark.sql.types import StructType, StructField, StringType, LongType

ORDER = StructType([
    StructField("order_id", LongType(), False),
    StructField("order_natural_key", StringType(), False)
])