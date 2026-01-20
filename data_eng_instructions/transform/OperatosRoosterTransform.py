from pyspark.sql import DataFrame
from pyspark.sql.functions import *

from data_eng_instructions.schema.file.OperatorsRooster import OPERATORS_ROOSTER_CSV, OPERATORS_ROOSTER


def csv_to_type(dataframe: DataFrame) -> DataFrame:
    assert dataframe.schema == OPERATORS_ROOSTER_CSV
    return (dataframe
    .withColumn("certifications", split(col("certifications"), ",").alias("certifications"))
    .withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))
    .withColumn(
        "overtime_eligible",
        when(lower(col("overtime_eligible")) == "true", True)
        .when(lower(col("overtime_eligible")) == "false", False)
        .otherwise(None)
        .cast("boolean")
    )
    .withColumn("hourly_rate_eur", col("hourly_rate_eur").cast("float"))
    .withColumn("reliability_score", col("reliability_score").cast("float"))
    .select(
        "operator_id",
        "name",
        "factory_id",
        "primary_line",
        "primary_shift",
        "skill_level",
        "certifications",
        "team",
        "hire_date",
        "overtime_eligible",
        "hourly_rate_eur",
        "reliability_score"
    )
    ).toDF(*OPERATORS_ROOSTER.fieldNames())
