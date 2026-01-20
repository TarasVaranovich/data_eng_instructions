from pyspark.sql.types import StructType, StructField, StringType, LongType

PRODUCT= StructType([
    StructField("product_id", LongType(), False),
    StructField("product_natural_key", StringType(), False)
])