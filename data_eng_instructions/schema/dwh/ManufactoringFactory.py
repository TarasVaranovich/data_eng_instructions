from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, DateType, \
    LongType, ShortType

MANUFACTURING_FACTORY_FACT = StructType([
    # Dimensions
    StructField("operating_period_id", LongType(), False),
    StructField("line_factory_id", LongType(), False),
    StructField("product_id", LongType(), False),
    StructField("order_id", LongType(), False),
    StructField("shift_id", LongType(), False),
    StructField("machine_state_id", ShortType(), False),
    StructField("operator_id", IntegerType(), False),  # enum
    StructField("workorder_status", IntegerType(), False),
    # Fact measures
    StructField("timestamp", TimestampType(), False),
    StructField("planned_qty", IntegerType(), False),
    StructField("produced_qty", IntegerType(), False),
    StructField("scrap_qty", IntegerType(), False),
    StructField("defects_count", IntegerType(), False),
    StructField("defect_type", StringType(), True),  # enum
    StructField("cycle_time_s", FloatType(), False),
    StructField("oee", FloatType(), False),
    StructField("availability", FloatType(), False),
    StructField("performance", FloatType(), False),
    StructField("quality", FloatType(), False),
    StructField("downtime_reason", StringType(), True),  # enum
    StructField("maintenance_type", StringType(), False),  # enum
    StructField("maintenance_due_date", DateType(), False),
    StructField("vibration_mm_s", FloatType(), False),
    StructField("temperature_c", FloatType(), False),
    StructField("pressure_bar", FloatType(), False)
])
