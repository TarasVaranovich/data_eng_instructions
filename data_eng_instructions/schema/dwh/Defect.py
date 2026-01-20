from pyspark.sql.types import StructType, StructField, IntegerType, StringType

OPERATING_PERIOD_DEFECT = StructType([
    StructField("operating_period_id", IntegerType(), False),
    StructField("defect_id", IntegerType(), False)
])

DEFECT = StructType([
    StructField("defect_id", IntegerType(), False),
    StructField("defect_type", StringType(), False)
])