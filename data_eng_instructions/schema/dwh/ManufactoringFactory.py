from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType, DateType, \
    LongType, ShortType

MANUFACTURING_FACTORY_FACT = StructType([
    # operating_period_id
    StructField("operating_period_id", LongType(), False),
    # line_factory_id
    StructField("line_factory_id", LongType(), False),
    # product_id
    StructField("product_id", LongType(), False),
    # order_id
    StructField("order_id", LongType(), False),
    # shift_id
    StructField("shift_id", LongType(), False),
    # machine_state_id
    StructField("machine_state_id", ShortType(), False),
    # operator_id
    StructField("operator_id", IntegerType(), False),
    # work_order_status_id
    StructField("work_order_status_id", IntegerType(), False),
    # timestamp
    StructField("timestamp", TimestampType(), False),
    # planned_qty
    StructField("planned_qty", IntegerType(), False),
    # produced_qty
    StructField("produced_qty", IntegerType(), False),
    # scrap_qty
    StructField("scrap_qty", IntegerType(), False),
    # cycle_time_s
    StructField("cycle_time_s", FloatType(), False),
    # oee
    StructField("oee", FloatType(), False),
    # availability
    StructField("availability", FloatType(), False),
    # performance
    StructField("performance", FloatType(), False),
    # quality
    StructField("quality", FloatType(), False),
    # maintenance_type_id
    StructField("maintenance_type_id", IntegerType(), False),
    # maintenance_due_date
    StructField("maintenance_due_date", DateType(), False),
    # vibration_mm_s
    StructField("vibration_mm_s", FloatType(), False),
    # temperature_c
    StructField("temperature_c", FloatType(), False),
    # pressure_bar
    StructField("pressure_bar", FloatType(), False),
    # energy_kwh
    StructField("energy_kwh", FloatType(), False)
])
