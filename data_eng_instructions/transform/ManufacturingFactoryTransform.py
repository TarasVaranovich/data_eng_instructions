from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from data_eng_instructions.type.ManufacturingFactory import MANUFACTURING_FACTORY_CSV


def csv_to_type(dataframe: DataFrame) -> DataFrame:
    assert dataframe.schema == MANUFACTURING_FACTORY_CSV
    return (dataframe
    .withColumn("timestamp",
                when(col("timestamp").isNull() | (trim(col("timestamp")) == ""),
                     raise_error("REQUIRED: timestamp cannot be null/empty"))
                .otherwise(to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")))

    .withColumn("planned_qty",
                when(col("planned_qty").isNull() | (trim(col("planned_qty")) == ""),
                     raise_error("REQUIRED: planned_qty must be valid integer"))
                .otherwise(col("planned_qty").cast(IntegerType())))

    .withColumn("produced_qty",
                when(col("produced_qty").isNull() | (trim(col("produced_qty")) == ""),
                     raise_error("REQUIRED: produced_qty must be valid integer"))
                .otherwise(col("produced_qty").cast(IntegerType())))

    .withColumn("scrap_qty",
                when(col("scrap_qty").isNull() | (trim(col("scrap_qty")) == ""),
                     raise_error("REQUIRED: scrap_qty must be valid integer"))
                .otherwise(col("scrap_qty").cast(IntegerType())))

    .withColumn("defects_count",
                when(col("defects_count").isNull() | (trim(col("defects_count")) == ""),
                     raise_error("REQUIRED: defects_count must be valid integer"))
                .otherwise(col("defects_count").cast(IntegerType())))

    .withColumn("maintenance_due_date",
                when(col("maintenance_due_date").isNull() | (trim(col("maintenance_due_date")) == ""),
                     raise_error("REQUIRED: maintenance_due_date must be valid YYYY-MM-DD"))
                .otherwise(to_date(col("maintenance_due_date"), "yyyy-MM-dd")))

    .withColumn("cycle_time_s", col("cycle_time_s").cast(FloatType()))
    .withColumn("oee", col("oee").cast(FloatType()))
    .withColumn("availability", col("availability").cast(FloatType()))
    .withColumn("performance", col("performance").cast(FloatType()))
    .withColumn("quality", col("quality").cast(FloatType()))
    .withColumn("vibration_mm_s", col("vibration_mm_s").cast(FloatType()))
    .withColumn("temperature_c", col("temperature_c").cast(FloatType()))
    .withColumn("pressure_bar", col("pressure_bar").cast(FloatType()))

    .withColumn("defect_type",
                coalesce(col("defect_type"), lit(None).cast(StringType())))
    .withColumn("downtime_reason",
                coalesce(col("downtime_reason"), lit(None).cast(StringType())))

    .withColumn("factory_id", trim(col("factory_id")))
    .withColumn("line_id", trim(col("line_id")))
    .withColumn("shift", trim(col("shift")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("machine_state", trim(col("machine_state")))
    .withColumn("maintenance_type", trim(col("maintenance_type")))
    .withColumn("operator_id", trim(col("operator_id")))
    .withColumn("workorder_status", trim(col("workorder_status")))

    .select(
        "timestamp",
        "factory_id",
        "line_id",
        "shift",
        "product_id",
        "order_id",
        "planned_qty",
        "produced_qty",
        "scrap_qty",
        "defects_count",
        "defect_type",
        "cycle_time_s",
        "oee",
        "availability",
        "performance",
        "quality",
        "machine_state",
        "downtime_reason",
        "maintenance_type",
        "maintenance_due_date",
        "vibration_mm_s",
        "temperature_c",
        "pressure_bar",
        "operator_id",
        "workorder_status"
    )
    )


def filter_out_invalid_defects(dataframe: DataFrame) -> DataFrame:
    assert dataframe.schema == MANUFACTURING_FACTORY_CSV
    return dataframe.filter((col("defect_type").isNotNull() & (col("defect_count") > 0)))
