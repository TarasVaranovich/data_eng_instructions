from pyspark.sql.types import StructType, StructField, StringType, ShortType

MACHINE_STATE = StructType([
    StructField("machine_state_id", ShortType(), False),
    StructField("machine_state", StringType(), False)
])