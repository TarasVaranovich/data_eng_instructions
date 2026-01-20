from pyspark.sql.types import StructType, StructField, StringType, IntegerType

SHIFT = StructType([
    StructField("shift_id", IntegerType(), False),
    StructField("shift_name", StringType(), False)
])