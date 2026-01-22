from pyspark.sql.types import StructType, StructField, StringType, IntegerType

MAINTENANCE_TYPE = StructType([
    StructField("maintenance_type_id", IntegerType(), False),
    StructField("maintenance_type", StringType(), False)
])