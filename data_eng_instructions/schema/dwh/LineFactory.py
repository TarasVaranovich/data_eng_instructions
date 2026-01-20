from pyspark.sql.types import StructType, StructField, StringType, IntegerType

LINE_FACTORY = StructType([
    StructField("line_factory_id", IntegerType(), False),
    StructField("line_natural_key", StringType(), False),
    StructField("factory_natural_key", StringType(), False)
])
